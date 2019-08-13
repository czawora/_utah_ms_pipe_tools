
import sys
import os
import glob
from subprocess import call
import pandas as pd

# max number of attempts to produce all outputs from sorting.
max_attempts = 6

# inputs:
# - full path of sort directory for channel
# - full path to session directory for channel

# typical call: python3 check_sort_completion.py /data/FRNU/CZ/in_progress/NIH064/181006_0029_INST0/spike/splits_sort/refset2_026 /data/FRNU/CZ/in_progress/NIH064/181006_0029_INST0

# The purpose of this function is to check if a single channel sort job completed and produced all outputs. It does so
# by checking that certain strings are present in the output log files. This is necessary for two reasons. One reason
# is that we only want to run the spikeInfo creation script once all the jobs from this NSP are finished.
# The second is that, for strange reasons, moutainsort binaries run on biowulf, specifcally the sort-related ones not the filtering ones,
# randomly fail. Luckily they are quick to rerun and usually work on the 2nd or 3rd try. This script sets the maximum number
# of tries and reruns scripts that are missing some outputs.


# get inputs
if len(sys.argv) < 3:
	print("first argument should be single channel sort path, second argument should be session path")
	exit(-1)

chan_dir = sys.argv[1]
session_dir = sys.argv[2]

# is chan_dir valid?
if os.path.isdir(chan_dir) is False:
	print(chan_dir + " is not a valid directory")
	exit(-1)


# initiate the file that counts the attempts to sort this channel
# OR read in the current attempt number from the existing file
count_fpath = chan_dir + "/attempts.log"
if os.path.isfile(count_fpath) is False:
	attempt_num = 1
else:

	count_file = open(count_fpath)

	top_line = count_file.readline().strip("\n")
	attempt_num = int(top_line)

	count_file.close()


# patterns for the output file logs
sort_log_fname = "_sort_%d.log" % attempt_num
metrics_log_fname = "_metrics_%d.log" % attempt_num
isol_metrics_log_fname = "_isol_metrics_%d.log" % attempt_num
raw_clips_log_fname = "_raw_clips_%d.log" % attempt_num
sort_clips_log_fname = "_sort_clips_%d.log" % attempt_num
spike_clips_log_fname = "_spike_clips_%d.log" % attempt_num
features_log_fname = "_features_%d.log" % attempt_num

# list of tuples containing the searched 'completion' strings for each required output
target_strings = []
target_strings.append(("ms_sort", "Process completed successfully: mountainsortalg.ms3alg", ["firings.mda"], sort_log_fname))
target_strings.append(("isol_metrics", "Process completed successfully: ms3.isolation_metrics", ["isol_metrics.json", "isol_pair_metrics.json"], isol_metrics_log_fname))
target_strings.append(("clips_raw", "Process completed successfully: ms3.mv_extract_clips", ["clips_raw.mda"], raw_clips_log_fname))
target_strings.append(("clips_spike", "Process completed successfully: ms3.mv_extract_clips", ["clips_spike.mda"], spike_clips_log_fname))
target_strings.append(("clips_whiten", "Process completed successfully: ms3.mv_extract_clips", ["clips_whiten.mda"], sort_clips_log_fname))
target_strings.append(("metrics", "Process completed successfully: ms3.cluster_metrics", ["metrics.json"], metrics_log_fname))
target_strings.append(("clip_features", "Process completed successfully: ms3.mv_extract_clips_features", ["clip_features.mda"], features_log_fname))

present_targets = []
missing_targets = []

# loop through each of those required outputs for this channel
for t in target_strings:

	# unpack the tuple
	tag = t[0]
	complete_string = t[1]
	output_files = t[2]
	log_files = t[3]

	# read the log file for this process
	log_contents = ""
	if os.path.isfile(chan_dir + "/" + log_files):

		log_fid = open(chan_dir + "/" + log_files)

		for l in log_fid:
			log_contents += l.strip("\n")

		log_fid.close()

	# are all the outputs there? A certain completion string may be searched in more than one output file
	# currently this happens in looking for the isol metrics
	all_output_present = True
	for of in output_files:
		if os.path.isfile(chan_dir + "/" + of) is False:
			all_output_present = False

	# check if the completion string is present in the log file
	if complete_string in log_contents and all_output_present is True:

		print("present: " + tag)
		present_targets.append(tag)

	else:

		print("missing: " + tag)
		missing_targets.append(tag)

		if attempt_num < max_attempts:

			# we are missing an output, delete this directory of sort results
			# in preparation of a rerun
			for of in output_files:
				if os.path.isfile(chan_dir + "/" + of) is True:
					os.remove(chan_dir + "/" + of)


# all the outputs are present for this channel
if len(present_targets) == len(target_strings):

	# create a marker file, indicating completion of this channel
	print("all done")
	f = open(chan_dir + "/done.log", 'w')
	f.close()

	# check to run spikeInfo - only want to do this if all channels are finished

	# count how many used channels there are
	used_jacksheet_glob = glob.glob(session_dir + "/combined_used_jacksheet.csv")

	used_jacksheet = pd.read_csv(used_jacksheet_glob[0])
	num_used_chans = used_jacksheet.shape[0]

	print(num_used_chans)

	# count done.logs
	split_dir = "/".join(chan_dir.split("/")[0:-1])
	done_files = glob.glob(split_dir + "/*/done.log")

	print(done_files)
	print(len(done_files))

	# once all the channels have a done file present, we can run the spikeInfo script
	if num_used_chans == len(done_files):

		# for debugging, list the done files found
		done_files.sort()

		spikeInfo_log = open(session_dir + "/_calling_spikeInfo.log", "w")

		for f in done_files:
			spikeInfo_log.write(f + "\n")
		spikeInfo_log.close()

		# submit the spikeInfo script to the cluster
		call(["sbatch", session_dir + "/spikeInfo.sh"])

# this means that some outputs failed to be produced. need to rerun, if below attempt threshold
else:

	if attempt_num < max_attempts:

		# accumulate attempt count
		count_file = open(count_fpath, 'w')
		count_file.write(str(attempt_num + 1))
		count_file.close()

		print("reruning " + chan_dir)

		call(["echo", chan_dir + "/sort.sh"])
		call(["bash", chan_dir + "/sort.sh"])
