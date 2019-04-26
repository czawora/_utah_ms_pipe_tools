

import os
import sys
import glob

sort_clip_size = 50
extract_clip_size = 300
num_features = 2

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

sys.path.append(dname)

if os.path.isfile(dname + "/paths.py") is False:
	print("move a copy of paths.py into this folder: " + dname)
	exit(2)

from paths import *

sbatch_header = []
sbatch_header.append("#!/bin/bash")
sbatch_header.append("#SBATCH --cpus-per-task=5")
sbatch_header.append("#SBATCH --gres=lscratch:1")
sbatch_header.append("#SBATCH --error=/dev/null")
sbatch_header.append("#SBATCH --output=/dev/null")


if len(sys.argv) < 3:
	print("first argument should be session path, second should be refset num")
	exit(-2)

session_dir = sys.argv[1]
refset = sys.argv[2]

# is session_dir valid?
if os.path.isdir(session_dir) is False:
	print(session_dir + " is not a valid directory")
	exit(-1)

if os.path.isdir(session_dir + "/log_dump") is False:
	os.mkdir(session_dir + "/log_dump")

session_swarm = open(session_dir + "/sort_swarm%s.sh" % refset, 'w')
session_swarm.write("swarm -g 10 -b 1 -t 2 --partition norm --gres=lscratch:2 --time 2:00:00 --merge-output --logdir " + session_dir + "/log_dump -f " + session_dir + "/sort_big_bash%s.sh" % refset)
session_swarm.close()

session_big_bash = open(session_dir + "/sort_big_bash%s.sh" % refset, 'w')

split_mda = glob.glob(session_dir + "/splits_sort/*/*refset%s*mda_chan" % refset)

for mda_fpath in split_mda:

	mda_splits = mda_fpath.split("/")
	mda_fname = mda_splits[-1]
	mda_path = "/".join(mda_splits[0:-1])

	channel_num = mda_splits[-2].split("_")[-1]
	session_name = mda_splits[-5]
	session_path = "/".join(mda_splits[0:-3])

	# find raw channel split file
	raw_chan_fpath = glob.glob(session_dir + "/splits_raw/*/" + mda_fname)
	spike_chan_fpath = glob.glob(session_dir + "/splits_spike/*/" + mda_fname)

	for log_file in glob.glob(mda_path + "/*.log"):
		os.remove(log_file)

	# time log file
	time_log_fpath = mda_path + "/time.log"

	# obligatory geom.csv file
	geom_fpath = mda_path + "/geom.csv"
	geom_file = open(geom_fpath, 'w')
	geom_file.write("0,0")
	geom_file.close()

	# write the sort file
	sub_cmd_fname = "sort.sh"
	sub_cmd_fpath = mda_path + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	session_big_bash.write("echo \"" + sub_cmd_fpath + "\";bash " + sub_cmd_fpath + "\n")

	# write the sbatch header for sub_cmd bash file
	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("source " + MS_env_source + "\n\n")

	sub_cmd_file.write("cp -r " + mountainsort_binaries_dir + " /lscratch/$SLURM_JOB_ID\n")
	sub_cmd_file.write("export PATH=/lscratch/$SLURM_JOB_ID:$PATH\n\n")

	sub_cmd_file.write("echo \"" + mda_fpath + "\"\n\n")

	sub_cmd_file.write("attempt_num=\"1\"\n")
	sub_cmd_file.write("if [ -f " + mda_path + "/attempts.log ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("attempt_num=`head -c 8 " + mda_path + "/attempts.log`\n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("total_log_fname=\"_total_$attempt_num.log\"\n")
	sub_cmd_file.write("sort_log_fname=\"_sort_$attempt_num.log\"\n")
	sub_cmd_file.write("metrics_log_fname=\"_metrics_$attempt_num.log\"\n")
	sub_cmd_file.write("isol_metrics_log_fname=\"_isol_metrics_$attempt_num.log\"\n")
	sub_cmd_file.write("raw_clips_log_fname=\"_raw_clips_$attempt_num.log\"\n")
	sub_cmd_file.write("sort_clips_log_fname=\"_sort_clips_$attempt_num.log\"\n")
	sub_cmd_file.write("spike_clips_log_fname=\"_spike_clips_$attempt_num.log\"\n")
	sub_cmd_file.write("features_log_fname=\"_features_$attempt_num.log\"\n")

	sub_cmd_file.write("if [ ! -f " + mda_path + "/attempts.log ]\n")
	sub_cmd_file.write("then\n\n")

	# remove old result
	sort_out_fpath = mda_path + "/firings.mda"

	sub_cmd_file.write("if [ -f " + sort_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + sort_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	metrics_out_fpath = mda_path + "/metrics.json"
	sub_cmd_file.write("if [ -f " + metrics_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + metrics_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	isol_metrics_out_fpath = mda_path + "/isol_metrics.json"
	isol_pair_metrics_out_fpath = mda_path + "/isol_pair_metrics.json"

	# remove old results
	sub_cmd_file.write("if [ -f " + isol_metrics_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + isol_metrics_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("if [ -f " + isol_pair_metrics_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + isol_pair_metrics_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	clips_whiten_out_fpath = mda_path + "/clips_whiten.mda"
	clips_raw_out_fpath = mda_path + "/clips_raw.mda"
	clips_spike_out_fpath = mda_path + "/clips_spike.mda"

	# remove old result
	sub_cmd_file.write("if [ -f " + clips_whiten_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + clips_whiten_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("if [ -f " + clips_raw_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + clips_raw_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("if [ -f " + clips_spike_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + clips_spike_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	features_out_fpath = mda_path + "/clip_features.mda"

	sub_cmd_file.write("if [ -f " + features_out_fpath + " ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm " + features_out_fpath + "\n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("#closing fi for attempts.log check\n")
	sub_cmd_file.write("fi\n\n")

	#################################
	# mountainsortalg.ms3alg
	#################################

	# make command
	sub_cmd_ms = []
	sub_cmd_ms.append("mp-run-process")
	sub_cmd_ms.append("mountainsortalg.ms3alg")
	sub_cmd_ms.append("--timeseries=" + mda_fpath)
	sub_cmd_ms.append("--geom=" + geom_fpath)
	sub_cmd_ms.append("--firings_out=" + sort_out_fpath)
	sub_cmd_ms.append("--adjacency_radius=0")
	sub_cmd_ms.append("--detect_sign=-1")
	# sub_cmd_ms.append("--merge_across_channels=false")
	sub_cmd_ms.append("--fit_stage=false")
	sub_cmd_ms.append("--clip_size=" + str(sort_clip_size))

	sub_cmd_ms.append("&> " + mda_path + "/$sort_log_fname")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#ms3alg\n")
	sub_cmd_file.write("################################\n")

	# timing
	sub_cmd_file.write("start_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"" + mda_path + ":start_msAlg:$start_time\" >> " + time_log_fpath + "\n\n")

	sub_cmd_file.write(" ".join(sub_cmd_ms) + "\n")
	sub_cmd_file.write("cat " + mda_path + "/$sort_log_fname >> " + mda_path + "/$total_log_fname\n")

	sub_cmd_file.write("done_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"" + mda_path + ":done_msAlg:$done_time\" >> " + time_log_fpath + ";\n\n")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#only continue if firings.mda output file exists\n")
	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("################################\n\n")

	sub_cmd_file.write("if [ -f " + sort_out_fpath + " ]\n")
	sub_cmd_file.write("then\n\n")

	################################
	# run metrics
	################################

	# make command
	sub_cmd_met = []
	sub_cmd_met.append("mp-run-process")
	sub_cmd_met.append("ms3.cluster_metrics")
	sub_cmd_met.append("--timeseries=" + mda_fpath)
	sub_cmd_met.append("--firings=" + sort_out_fpath)
	sub_cmd_met.append("--cluster_metrics_out=" + metrics_out_fpath)
	sub_cmd_met.append("--samplerate=30000")

	sub_cmd_met.append("&>> " + mda_path + "/$metrics_log_fname")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#run metrics\n")
	sub_cmd_file.write("################################\n")

	# timing
	sub_cmd_file.write("start_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
	sub_cmd_file.write("echo \"" + mda_path + ":start_metrics:$start_time\" >> " + time_log_fpath + "\n\n")

	sub_cmd_file.write(" ".join(sub_cmd_met) + "\n")
	sub_cmd_file.write("cat " + mda_path + "/$metrics_log_fname >> " + mda_path + "/$total_log_fname\n")

	sub_cmd_file.write("done_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"" + mda_path + ":done_metrics:$done_time\" >> " + time_log_fpath + ";\n\n")

	################################
	# run isolation metrics
	################################

	# make commmand
	sub_cmd_isol = []
	sub_cmd_isol.append("mp-run-process")
	sub_cmd_isol.append("ms3.isolation_metrics")
	sub_cmd_isol.append("--timeseries=" + mda_fpath)
	sub_cmd_isol.append("--firings=" + sort_out_fpath)
	sub_cmd_isol.append("--metrics_out=" + isol_metrics_out_fpath)
	sub_cmd_isol.append("--pair_metrics_out=" + isol_pair_metrics_out_fpath)
	sub_cmd_isol.append("--compute_bursting_parents=true")

	sub_cmd_isol.append("&>> " + mda_path + "/$isol_metrics_log_fname")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#run isolation metrics\n")
	sub_cmd_file.write("################################\n")

	# timing
	sub_cmd_file.write("start_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
	sub_cmd_file.write("echo \"" + mda_path + ":start_isolMetrics:$start_time\" >> " + time_log_fpath + "\n\n")

	sub_cmd_file.write(" ".join(sub_cmd_isol) + "\n")
	sub_cmd_file.write("cat " + mda_path + "/$isol_metrics_log_fname >> " + mda_path + "/$total_log_fname\n")

	sub_cmd_file.write("done_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"" + mda_path + ":done_isolMetrics:$done_time\" >> " + time_log_fpath + ";\n\n")

	################################
	# run extract_clips
	################################

	sub_cmd_clips = []
	sub_cmd_clips.append("mp-run-process")
	sub_cmd_clips.append("ms3.mv_extract_clips")
	sub_cmd_clips.append("--timeseries=" + mda_fpath)
	sub_cmd_clips.append("--firings=" + sort_out_fpath)
	sub_cmd_clips.append("--clips_out=" + clips_whiten_out_fpath)
	sub_cmd_clips.append("--clip_size=" + str(extract_clip_size))

	sub_cmd_clips.append("&>> " + mda_path + "/$sort_clips_log_fname")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#run extract_clips\n")
	sub_cmd_file.write("################################\n")

	# timing
	sub_cmd_file.write("start_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
	sub_cmd_file.write("echo \"" + mda_path + ":start_clips:$start_time\" >> " + time_log_fpath + ";\n\n")

	sub_cmd_file.write(" ".join(sub_cmd_clips) + "\n")
	sub_cmd_file.write("cat " + mda_path + "/$sort_clips_log_fname >> " + mda_path + "/$total_log_fname\n")

	sub_cmd_file.write("done_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"" + mda_path + ":done_clips:$done_time\" >> " + time_log_fpath + ";\n\n")

	if raw_chan_fpath != []:

		sub_cmd_clips = []
		sub_cmd_clips.append("mp-run-process")
		sub_cmd_clips.append("ms3.mv_extract_clips")
		sub_cmd_clips.append("--timeseries=" + raw_chan_fpath[0])
		sub_cmd_clips.append("--firings=" + sort_out_fpath)
		sub_cmd_clips.append("--clips_out=" + clips_raw_out_fpath)
		sub_cmd_clips.append("--clip_size=" + str(extract_clip_size))

		sub_cmd_clips.append("&>> " + mda_path + "/$raw_clips_log_fname")

		sub_cmd_file.write("################################\n")
		sub_cmd_file.write("#run extract_clips\n")
		sub_cmd_file.write("################################\n")

		# timing
		sub_cmd_file.write("start_time=$(date +%s)\n")
		sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
		sub_cmd_file.write("echo \"" + raw_chan_fpath[0] + ":start_clips:$start_time\" >> " + time_log_fpath + ";\n\n")

		sub_cmd_file.write(" ".join(sub_cmd_clips) + "\n")
		sub_cmd_file.write("cat " + mda_path + "/$raw_clips_log_fname >> " + mda_path + "/$total_log_fname\n")

		sub_cmd_file.write("done_time=$(date +%s)\n")
		sub_cmd_file.write("echo \"" + raw_chan_fpath[0] + ":done_clips:$done_time\" >> " + time_log_fpath + ";\n\n")

	if spike_chan_fpath != []:

		sub_cmd_clips = []
		sub_cmd_clips.append("mp-run-process")
		sub_cmd_clips.append("ms3.mv_extract_clips")
		sub_cmd_clips.append("--timeseries=" + spike_chan_fpath[0])
		sub_cmd_clips.append("--firings=" + sort_out_fpath)
		sub_cmd_clips.append("--clips_out=" + clips_spike_out_fpath)
		sub_cmd_clips.append("--clip_size=" + str(extract_clip_size))

		sub_cmd_clips.append("&>> " + mda_path + "/$spike_clips_log_fname")

		sub_cmd_file.write("################################\n")
		sub_cmd_file.write("#run extract_clips\n")
		sub_cmd_file.write("################################\n")

		# timing
		sub_cmd_file.write("start_time=$(date +%s)\n")
		sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
		sub_cmd_file.write("echo \"" + spike_chan_fpath[0] + ":start_clips:$start_time\" >> " + time_log_fpath + ";\n\n")

		sub_cmd_file.write(" ".join(sub_cmd_clips) + "\n")
		sub_cmd_file.write("cat " + mda_path + "/$spike_clips_log_fname >> " + mda_path + "/$total_log_fname\n")

		sub_cmd_file.write("done_time=$(date +%s)\n")
		sub_cmd_file.write("echo \"" + spike_chan_fpath[0] + ":done_clips:$done_time\" >> " + time_log_fpath + ";\n\n")

	################################
	# run extract_clip_features
	################################

	# make command
	sub_cmd_feats = []
	sub_cmd_feats.append("mp-run-process")
	sub_cmd_feats.append("ms3.mv_extract_clips_features")
	sub_cmd_feats.append("--timeseries=" + mda_fpath)
	sub_cmd_feats.append("--firings=" + sort_out_fpath)
	sub_cmd_feats.append("--features_out=" + features_out_fpath)
	sub_cmd_feats.append("--clip_size=" + str(sort_clip_size))
	sub_cmd_feats.append("--num_features=" + str(num_features))
	sub_cmd_feats.append("--subtract_mean=" + "true")

	sub_cmd_feats.append("&>> " + mda_path + "/$features_log_fname")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#run extract_clip_features\n")
	sub_cmd_file.write("################################\n")

	# timing
	sub_cmd_file.write("start_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
	sub_cmd_file.write("echo \"" + mda_path + ":start_features:$start_time\" >> " + time_log_fpath + ";\n\n")

	sub_cmd_file.write(" ".join(sub_cmd_feats) + "\n")
	sub_cmd_file.write("cat " + mda_path + "/$features_log_fname >> " + mda_path + "/$total_log_fname\n")

	sub_cmd_file.write("done_time=$(date +%s)\n")
	sub_cmd_file.write("echo \"" + mda_path + ":done_features:$done_time\" >> " + time_log_fpath + ";\n\n")

	################################
	# run plotChannelSpikes
	################################

	# # make command
	# sub_cmd_feats = []
	# sub_cmd_feats.append("cd " + plotChannelSpikes_matlab_dir + "/_plotChannelSpikes; ./run_plotChannelSpikes_swarm.sh /usr/local/matlab-compiler/v94")
	# sub_cmd_feats.append("session_name")
	# sub_cmd_feats.append(session_name)
	# sub_cmd_feats.append("channel_num")
	# sub_cmd_feats.append(channel_num)
	# sub_cmd_feats.append("clip_features")
	# sub_cmd_feats.append(features_out_fpath)
	# sub_cmd_feats.append("clips")
	# sub_cmd_feats.append(clips_whiten_out_fpath)
	# sub_cmd_feats.append("firings")
	# sub_cmd_feats.append(sort_out_fpath)
	# sub_cmd_feats.append("isol_metrics")
	# sub_cmd_feats.append(isol_metrics_out_fpath)
	# sub_cmd_feats.append("isol_pair_metrics")
	# sub_cmd_feats.append(isol_pair_metrics_out_fpath)
	# sub_cmd_feats.append("metrics")
	# sub_cmd_feats.append(metrics_out_fpath)
	# sub_cmd_feats.append("mda")
	# sub_cmd_feats.append(mda_fpath)
	# sub_cmd_feats.append("saveDir")
	# sub_cmd_feats.append(session_path + "/outputs/sortFigs")
	#
	# sub_cmd_feats.append("&>> " + mda_path + "/$sort_log_fname")
	#
	# sub_cmd_file.write("################################\n")
	# sub_cmd_file.write("#run plotChannelSpikes\n")
	# sub_cmd_file.write("################################\n")
	#
	# # timing
	# sub_cmd_file.write("start_time=$(date +%s)\n")
	# sub_cmd_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
	# sub_cmd_file.write("echo \"" + mda_path + ":start_plotting:$start_time\" >> " + time_log_fpath + ";\n\n")
	#
	# sub_cmd_file.write(" ".join(sub_cmd_feats) + "\n\n")
	#
	# sub_cmd_file.write("done_time=$(date +%s)\n")
	# sub_cmd_file.write("echo \"" + mda_path + ":done_plotting:$done_time\" >> " + time_log_fpath + ";\n\n")
	#
	sub_cmd_file.write("\n\n")
	sub_cmd_file.write("###closing 'fi' for if statement checking presence of firings.mda output file\n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("################################\n")
	sub_cmd_file.write("#check for completion\n")
	sub_cmd_file.write("################################\n\n")

	sub_cmd_file.write("/data/FRNU/installs/install_python/bin/python3 " + spikes_pipeline_dir + "/check_sort_completion.py " + mda_path + " " + session_dir + " &>> " + mda_path + "/$total_log_fname\n\n")

	sub_cmd_file.close()

session_big_bash.close()
