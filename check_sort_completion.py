
import sys
import os
import glob
from subprocess import call

max_attempts = 6

target_strings = []
target_strings.append(("ms_sort", "Process completed successfully: mountainsortalg.ms3alg", ["firings.mda"] ))
target_strings.append(("isol_metrics", "Process completed successfully: ms3.isolation_metrics", ["isol_metrics.json", "isol_pair_metrics.json"] ))
target_strings.append(("clips", "Process completed successfully: ms3.mv_extract_clips", ["clips.mda"] ))
target_strings.append(("metrics", "Process completed successfully: ms3.cluster_metrics", ["metrics.json"] ))
target_strings.append(("clip_features", "Process completed successfully: ms3.mv_extract_clips_features", ["clip_features.mda"] ))
# target_strings.append(("plot", "plotChannelSpikes -- done", []))

present_targets = []
missing_targets = []

if len(sys.argv) < 3:
	print("first argument should be single channel sort path, second should be 'seq' or 'par' indicating if run mode")
	exit(-1)

chan_dir = sys.argv[1]
run_mode = sys.argv[2]

if run_mode != "seq" and run_mode != "par":
	print("run mode must be either 'seq' or 'par'")
	exit(-2)

# is chan_dir valid?
if os.path.isdir(chan_dir) is False:
	print(chan_dir + " is not a valid directory")
	exit(-1)

count_fpath = chan_dir + "/attempts.log"
if os.path.isfile(count_fpath) is False:

	attempt_num = 1
else:

	count_file = open(count_fpath)

	top_line = count_file.readline().strip("\n")
	attempt_num = int(top_line)

	count_file.close()


sort_log = ""
sort_log_file = open(chan_dir + "/_sort_" + str(attempt_num) + ".log")

for l in sort_log_file:
	sort_log += l.strip("\n")

for t in target_strings:

	tag = t[0]
	complete_string = t[1]
	output_files = t[2]

	if complete_string in sort_log:

		print("present: " + tag)
		present_targets.append(tag)

	else:

		print("missing: " + tag)
		missing_targets.append(tag)

		if attempt_num < max_attempts:

			for of in output_files:
				if os.path.isfile(chan_dir + "/" + of) is True:
					os.remove(chan_dir + "/" + of)


if len(present_targets) == len(target_strings):

	print("check_sort_completion - all done")
	f = open(chan_dir + "/done.log", 'w')
	f.close()

	# check to run spikeInfo

	# count how many used channels there are
	# get session dir
	session_spike_dir = "/".join(chan_dir.split("/")[0:-2])
	used_chan_glob = glob.glob(session_spike_dir + "/*refset*_used_chans.txt")

	used_chans = []

	for used_chan_fname in used_chan_glob:

		used_chan_file = open(used_chan_fname)

		for l in used_chan_file:

			line = l.strip("\n")

			if line != "":

				used_chans.append(line)

		used_chan_file.close()

	# print(used_chans)
	num_used_chans = len(used_chans)
	# print(num_used_chans)

	# count done.logs

	split_dir = "/".join(chan_dir.split("/")[0:-1])

	done_files = glob.glob(split_dir + "/*/done.log")

	done_files.sort()
	for d in done_files:
		print(d)

	print(len(done_files))
	if num_used_chans == len(done_files):

		used_chans.sort()
		done_files.sort()

		spikeInfo_log = open(session_spike_dir + "/_calling_spikeInfo.sh", "w")

		for idx, f in enumerate(used_chans):
			spikeInfo_log.write(used_chans[idx] + "\t" + done_files[idx] + "\n")
		spikeInfo_log.close()

		call(["sbatch", session_spike_dir + "/spikeInfo.sh"])


else:

	if attempt_num < max_attempts:

		count_file = open(count_fpath, 'w')
		count_file.write(str(attempt_num + 1))
		count_file.close()

		print("reruning " + chan_dir)

		if run_mode == "par":
			call(["sbatch", chan_dir + "/sort.sh"])
		elif run_mode == "seq":
			call(["echo", chan_dir + "/sort.sh"])
			call(["bash", chan_dir + "/sort.sh"])
