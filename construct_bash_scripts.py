
import os
import glob
import argparse
import math
import sys
import shutil
import pandas as pd

# directory setup
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

sys.path.append(dname)

if os.path.isfile(dname + "/paths.py") is False:
	print("move a copy of paths.py into this folder: " + dname)
	exit(2)

import paths

#########################################################################
# START FUNCTIONS #######################################################
#########################################################################


def write_nsx2mda(session_dir, nsx_fpath, jacksheet_fpath, refset):

	sub_cmd_fname = "nsx2mda%s.sh" % str(refset)
	sub_cmd_log_fname = "_nsx2mda%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=2")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("echo \"start nsx2mda\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	# convert to mda format
	matlab_command = "cd " + paths.nsx2mda_matlab_dir + "/_nsx2mda; ./run_nsx2mda_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("session_dir")
	sub_cmd.append(session_dir)
	sub_cmd.append("nsx_fpath")
	sub_cmd.append(nsx_fpath)
	sub_cmd.append("refset")
	sub_cmd.append(str(refset))
	sub_cmd.append("jacksheet_fpath")
	sub_cmd.append(jacksheet_fpath)
	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("echo \"end nsx2mda\"\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_bandpass_raw(session_dir, refset):

	sub_cmd_fname = "bandpass_raw%s.sh" % str(refset)
	sub_cmd_log_fname = "_bandpass_raw%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)

	# write the sbatch header for sub_cmd bash file
	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("source " + paths.MS_env_source + "\n\n")

	sub_cmd_file.write("echo \"start bandpass_raw\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("bandpass_input_nsx=`ls " + session_dir + "/* | grep \"refset%s\.mda_raw_reref$\"`\n" % str(refset))
	sub_cmd_file.write("bandpass_output_mda=${bandpass_input_nsx}_bp\n\n")

	sub_cmd_file.write("echo \"input:${bandpass_input_nsx}\"\n")
	sub_cmd_file.write("echo \"output:${bandpass_output_mda}\"\n\n")

	sub_cmd_file.write("if [ -f $bandpass_output_mda  ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm $bandpass_output_mda \n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("export HOME=/lscratch/$SLURM_JOB_ID\n")
	sub_cmd_file.write("/data/FRNU/installs/install_python/bin/python3 " + paths.spikes_pipeline_dir + "/make_local_mlconfig.py $SLURM_JOB_ID\n")

	sub_cmd = []
	sub_cmd.append(paths.mountainsort_binaries_dir + "/mp-run-process")
	sub_cmd.append("ms3.bandpass_filter")
	sub_cmd.append("--timeseries=${bandpass_input_nsx}")
	sub_cmd.append("--timeseries_out=${bandpass_output_mda}")
	sub_cmd.append("--samplerate=30000")
	sub_cmd.append("--freq_min=1")
	sub_cmd.append("--freq_max=5000")

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write("for i in `seq 1 5`;\n")
	sub_cmd_file.write("do\n")
	sub_cmd_file.write("if [ ! -f ${bandpass_output_mda} ]; then\n")

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("fi\n")
	sub_cmd_file.write("done\n")

	sub_cmd_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_raw_reref$\"`\n\n" % str(refset))
	sub_cmd_file.write("rm -r /lscratch/$SLURM_JOB_ID/ms_tmp/*\n")

	sub_cmd_file.write("echo \"end bandpass_raw\"\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_bandpass_spike(session_dir, refset):

	sub_cmd_fname = "bandpass_spike%s.sh" % str(refset)
	sub_cmd_log_fname = "_bandpass_spike%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)

	# write the sbatch header for sub_cmd bash file
	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("source " + paths.MS_env_source + "\n\n")

	sub_cmd_file.write("echo \"start bandpass_spike\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("bandpass_input_nsx=`ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref$\"`\n" % str(refset))
	sub_cmd_file.write("bandpass_output_mda=${bandpass_input_nsx}_bp\n\n")

	sub_cmd_file.write("echo \"input:${bandpass_input_nsx}\"\n")
	sub_cmd_file.write("echo \"output:${bandpass_output_mda}\"\n\n")

	sub_cmd_file.write("if [ -f $bandpass_output_mda  ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm $bandpass_output_mda \n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("export HOME=/lscratch/$SLURM_JOB_ID\n")
	sub_cmd_file.write("/data/FRNU/installs/install_python/bin/python3 " + paths.spikes_pipeline_dir + "/make_local_mlconfig.py $SLURM_JOB_ID\n")

	sub_cmd = []
	sub_cmd.append(paths.mountainsort_binaries_dir + "/mp-run-process")
	sub_cmd.append("ms3.bandpass_filter")
	sub_cmd.append("--timeseries=${bandpass_input_nsx}")
	sub_cmd.append("--timeseries_out=${bandpass_output_mda}")
	sub_cmd.append("--samplerate=30000")
	sub_cmd.append("--freq_min=600")
	sub_cmd.append("--freq_max=6000")

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write("for i in `seq 1 5`;\n")
	sub_cmd_file.write("do\n")
	sub_cmd_file.write("if [ ! -f ${bandpass_output_mda} ]; then\n")

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("fi\n")
	sub_cmd_file.write("done\n")

	sub_cmd_file.write("echo \"end bandpass_spike\"\n")

	sub_cmd_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref$\"`\n\n" % str(refset))
	sub_cmd_file.write("rm -r /lscratch/$SLURM_JOB_ID/ms_tmp/*\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_reref_raw(session_dir, refset):

	sub_cmd_fname = "reref_raw%s.sh" % str(refset)
	sub_cmd_log_fname = "_reref_raw%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("echo \"start reref_raw\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("bandpass_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda$\"`\n" % str(refset))
	sub_cmd_file.write("reref_output_mda=${bandpass_input_mda}_raw_reref\n\n")

	sub_cmd_file.write("echo \"input:${bandpass_input_mda}\"\n")
	sub_cmd_file.write("echo \"output:${reref_output_mda}\"\n\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.globalReref_allchan_matlab_dir + "/_globalReref_allchan; ./run_globalReref_allchan_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("$bandpass_input_mda")
	sub_cmd.append("$reref_output_mda")

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("echo \"end reref_raw\"\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_reref_spike(session_dir, refset):

	sub_cmd_fname = "reref_spike%s.sh" % str(refset)
	sub_cmd_log_fname = "_reref_spike%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("echo \"start reref_spike\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("bandpass_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda$\"`\n" % str(refset))
	sub_cmd_file.write("reref_output_mda=${bandpass_input_mda}_spike_reref\n\n")

	sub_cmd_file.write("echo \"input:${bandpass_input_mda}\"\n")
	sub_cmd_file.write("echo \"output:${reref_output_mda}\"\n\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.globalReref_allchan_matlab_dir + "/_globalReref_allchan; ./run_globalReref_allchan_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("$bandpass_input_mda")
	sub_cmd.append("$reref_output_mda")

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("echo \"end reref_spike\"\n")

	sub_cmd_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda$\"`\n\n" % str(refset))

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_split_raw(session_dir, refset):

	# write the sort file
	sub_cmd_fname = "split_raw%s.sh" % str(refset)
	sub_cmd_log_fname = "_split_raw%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("reref_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda_raw_reref_bp$\"`\n" % str(refset))
	sub_cmd_file.write("echo \"input:${reref_input_mda}\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	split_dir = session_dir + "/splits_raw"

	sub_cmd_file.write("if [ ! -d \"" + split_dir + "\" ]; then\n")
	sub_cmd_file.write("mkdir " + split_dir + "\n")
	sub_cmd_file.write("fi\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.splitmda_matlab_dir + "/_splitmda; ./run_splitmda_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("input_filename")
	sub_cmd.append("$reref_input_mda")
	sub_cmd.append("output_dir")
	sub_cmd.append(split_dir)
	sub_cmd.append("refset")
	sub_cmd.append(str(refset))

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_raw_reref_bp$\"`\n\n" % str(refset))

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_split_spike(session_dir, refset):

	# write the sort file
	sub_cmd_fname = "split_spike%s.sh" % str(refset)
	sub_cmd_log_fname = "_split_spike%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("reref_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref_bp$\"`\n" % str(refset))
	sub_cmd_file.write("echo \"input:${reref_input_mda}\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	split_dir = session_dir + "/splits_spike"

	sub_cmd_file.write("if [ ! -d \"" + split_dir + "\" ]; then\n")
	sub_cmd_file.write("mkdir " + split_dir + "\n")
	sub_cmd_file.write("fi\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.splitmda_matlab_dir + "/_splitmda; ./run_splitmda_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("input_filename")
	sub_cmd.append("$reref_input_mda")
	sub_cmd.append("output_dir")
	sub_cmd.append(split_dir)
	sub_cmd.append("refset")
	sub_cmd.append(str(refset))

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_split_sort(session_dir, refset):

	# write the sort file
	sub_cmd_fname = "split_sort%s.sh" % str(refset)
	sub_cmd_log_fname = "_split_sort%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file

	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("reref_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref_bp_whiten$\"`\n" % str(refset))
	sub_cmd_file.write("echo \"input:${reref_input_mda}\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	split_dir = session_dir + "/splits_sort"

	sub_cmd_file.write("if [ ! -d \"" + split_dir + "\" ]; then\n")
	sub_cmd_file.write("mkdir " + split_dir + "\n")
	sub_cmd_file.write("fi\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.splitmda_matlab_dir + "/_splitmda; ./run_splitmda_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)
	sub_cmd.append("input_filename")
	sub_cmd.append("$reref_input_mda")
	sub_cmd.append("output_dir")
	sub_cmd.append(split_dir)
	sub_cmd.append("refset")
	sub_cmd.append(str(refset))

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref_bp_whiten$\"`\n\n" % str(refset))

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_whiten_sort(session_dir, refset):

	sub_cmd_fname = "whiten_sort%s.sh" % str(refset)
	sub_cmd_log_fname = "_whiten_sort%s.log" % str(refset)
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=120g")
	sbatch_header.append("#SBATCH --cpus-per-task=10")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)

	# write the sbatch header for sub_cmd bash file
	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("source " + paths.MS_env_source + "\n\n")

	sub_cmd_file.write("echo \"start whiten_sort\"\n")
	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("whiten_input=`ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref_bp$\"`\n" % str(refset))
	sub_cmd_file.write("whiten_output_mda=${whiten_input}_whiten\n\n")

	sub_cmd_file.write("echo \"input:${whiten_input}\"\n")
	sub_cmd_file.write("echo \"output:${whiten_output_mda}\"\n\n")

	sub_cmd_file.write("if [ -f $whiten_output_mda  ]\n")
	sub_cmd_file.write("then\n")
	sub_cmd_file.write("\trm $whiten_output_mda \n")
	sub_cmd_file.write("fi\n\n")

	sub_cmd_file.write("export HOME=/lscratch/$SLURM_JOB_ID\n")
	sub_cmd_file.write("/data/FRNU/installs/install_python/bin/python3 " + paths.spikes_pipeline_dir + "/make_local_mlconfig.py $SLURM_JOB_ID\n")

	sub_cmd = []
	sub_cmd.append(paths.mountainsort_binaries_dir + "/mp-run-process")
	sub_cmd.append("ms3.whiten")
	sub_cmd.append("--timeseries=${whiten_input}")
	sub_cmd.append("--timeseries_out=${whiten_output_mda}")

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write("for i in `seq 1 5`;\n")
	sub_cmd_file.write("do\n")
	sub_cmd_file.write("if [ ! -f ${whiten_output_mda} ]; then\n")

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.write("fi\n")
	sub_cmd_file.write("done\n")

	sub_cmd_file.write("echo \"end whiten_sort\"\n")

	sub_cmd_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_spike_reref_bp$\"`\n\n" % str(refset))
	sub_cmd_file.write("rm -r /lscratch/$SLURM_JOB_ID/ms_tmp/*\n")

	sub_cmd_file.close()

	return(sub_cmd_fpath)


def write_spikeInfo(session_dir, combined_jacksheet_fpath, ns3_glob, nev_glob, partition, spikeInfo_mem):

	split_dir = session_dir + "/splits_sort"

	# write the sort file
	sub_cmd_fname = "spikeInfo.sh"
	sub_cmd_log_fname = "_spikeInfo.log"
	sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
	sub_cmd_file = open(sub_cmd_fpath, 'w')

	# write the sbatch header for sub_cmd bash file
	sbatch_header = []
	sbatch_header.append("#!/bin/bash")
	sbatch_header.append("#SBATCH --mem=" + spikeInfo_mem)
	sbatch_header.append("#SBATCH --partition=" + partition)
	sbatch_header.append("#SBATCH --cpus-per-task=1")
	sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
	sbatch_header.append("#SBATCH --time=24:00:00")
	sbatch_header.append("#SBATCH --gres=lscratch:15")

	for l in sbatch_header:
		sub_cmd_file.write(l + "\n")

	sub_cmd_file.write("\n\n")

	sub_cmd_file.write("rm -f `ls " + session_dir + "/outputs/* | grep \"_spikeInfo\.mat$\"`\n")
	sub_cmd_file.write("rm -f `ls " + session_dir + "/outputs/* | grep \"_sortSummary\.mat$\"`\n\n")

	sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

	sub_cmd_file.write("tar -C /lscratch/$SLURM_JOB_ID -xf /usr/local/matlab-compiler/v94.tar.gz;")

	matlab_command = "cd " + paths.construct_spikeInfoMS_matlab_dir + "/_construct_spikeInfoMS; ./run_construct_spikeInfoMS_swarm.sh " + paths.matlab_compiler_ver_str

	sub_cmd = []
	sub_cmd.append(matlab_command)

	sub_cmd.append("session_path")
	sub_cmd.append(session_dir)

	sub_cmd.append("split_path")
	sub_cmd.append(split_dir)

	if ns3_glob != []:
		sub_cmd.append("analog_pulse_fpath")
		sub_cmd.append(ns3_glob[0])  # ns3_fpath

	if nev_glob != []:
		sub_cmd.append("nev_fpath")
		sub_cmd.append(nev_glob[0])  # nev_fpath

	sub_cmd.append("saveRoot")
	sub_cmd.append(session_dir + "/outputs")

	sub_cmd.append("used_jacksheet_fpath")
	sub_cmd.append(combined_jacksheet_fpath)

	sub_cmd.append("&>> " + session_dir + "/" + sub_cmd_log_fname)

	sub_cmd_file.write(" ".join(sub_cmd) + "\n")

	sub_cmd_file.close()


def write_session_scripts(subj_path, sess, nsx_fpath, jacksheet_fpath, analog_pulse_ext, nsp_suffix, min_range_cutoff_millivolt, min_duration_minutes, delete_splits, fresh_write):

	session_dir = subj_path + "/" + sess + "/spike"
	nsx_filesize = os.path.getsize(nsx_fpath)

	if fresh_write is True:
		print(" (fresh_write) removing " + session_dir, end="")
		if os.path.isdir(session_dir) is True:
			shutil.rmtree(session_dir)

	if os.path.isdir(session_dir) is False:
		os.mkdir(session_dir)

	# filename templates
	bash_fname = "sort_run_all%s.sh"
	bash_log_fname = "_sort_run_all%s.log"

	# get pulse filepaths
	ns3_glob = glob.glob(subj_path + "/" + sess + "/*." + analog_pulse_ext)
	nev_glob = glob.glob(subj_path + "/" + sess + "/*.nev")

	# open the jacksheet and see how many microDevNums there are
	jacksheet = pd.read_csv(jacksheet_fpath)
	jacksheet_nsp_allmicro = jacksheet.loc[(jacksheet["NSPsuffix"] == nsp_suffix) & (jacksheet["MicroDevNum"] >= 1)]
	jacksheet_unique_dev_num = jacksheet_nsp_allmicro.MicroDevNum.unique().tolist()

	# filter micro channels that do not meet time and range criteria
	jacksheet_nsp_allmicro_filt = jacksheet_nsp_allmicro.loc[(jacksheet_nsp_allmicro["RangeMilliV"] >= min_range_cutoff_millivolt) & (jacksheet_nsp_allmicro["DurationMin"] >= min_duration_minutes)]

	combined_jacksheet_fpath = session_dir + "/combined_used_jacksheet.csv"
	if jacksheet_nsp_allmicro_filt.empty is False:
		jacksheet_nsp_allmicro_filt.to_csv(combined_jacksheet_fpath, index=False)

	# delete existing split files
	if delete_splits is True:
		for split_dir in glob.glob(session_dir + "/splits*"):
			print(" ... removing old split/ dir", end="")
			shutil.rmtree(split_dir)

	log_glob = glob.glob(session_dir + "/*.log")
	if log_glob != []:
		print(" ... removing old log files", end="")
		for log_file in log_glob:
			os.rename(log_file, log_file + ".old")

	command_tuple_list = []

	print(" ... dev nums: " + " ".join(map(str, jacksheet_unique_dev_num)), end="")

	write_spikeInfo_flag = True
	for irefset, refset in enumerate(jacksheet_unique_dev_num):

		jacksheet_filt_refset = jacksheet_nsp_allmicro_filt.loc[jacksheet_nsp_allmicro_filt["MicroDevNum"] == refset]

		if jacksheet_filt_refset.empty is True:

			ignore_fid = open(session_dir + "/ignore_me%d.txt" % refset, 'w')
			ignore_fid.write("channels from microDev " + str(refset) + " do not pass duration ( DurationMin >= " + str(min_duration_minutes) + ") and voltage range filters ( RangeMilliV >= " + str(min_range_cutoff_millivolt) + ")")
			ignore_fid.close()

		else:

			# save the used jacksheet
			refset_jacksheet_fpath = session_dir + "/jacksheet_refset%d.csv" % refset
			jacksheet_filt_refset.to_csv(refset_jacksheet_fpath, index=False)

			# set the bash templates to real name
			current_bash_fname = bash_fname % str(refset)
			current_bash_log_fname = bash_log_fname % str(refset)

			# sort by nsx size
			if nsx_filesize/1e9 < 25:

				partition = "norm"
				spikeInfo_mem = "200g"
				refset_bash_command = "bash " + session_dir + "/" + current_bash_fname
				command_tuple_list.append(("small", refset_bash_command))

			elif nsx_filesize/1e9 < 40:

				partition = "largemem"
				spikeInfo_mem = "500g"
				refset_bash_command = "bash " + session_dir + "/" + current_bash_fname
				command_tuple_list.append(("large", refset_bash_command))

			else:

				partition = "largemem"
				spikeInfo_mem = "500g"
				refset_bash_command = "bash " + session_dir + "/" + current_bash_fname
				command_tuple_list.append(("xlarge", refset_bash_command))

			time_log_fpath = session_dir + "/time.log"
			sort_sbatch_file = open(session_dir + "/" + current_bash_fname, 'w')

			# write the sbatch header for combo bash file
			sbatch_header = []
			sbatch_header.append("#!/bin/bash")
			sbatch_header.append("#SBATCH --mem=200g")
			sbatch_header.append("#SBATCH --cpus-per-task=5")
			sbatch_header.append("#SBATCH --error=" + session_dir + "/" + current_bash_log_fname)
			sbatch_header.append("#SBATCH --output=" + session_dir + "/" + current_bash_log_fname)
			sbatch_header.append("#SBATCH --time=10:00:00")
			sbatch_header.append("#SBATCH --gres=lscratch:15")

			for l in sbatch_header:
				sort_sbatch_file.write(l + "\n")

			sort_sbatch_file.write("\n\n")

			#################################
			#################################
			# load MS env
			#################################

			sort_sbatch_file.write("export done_time\n\n")

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#load ms env\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo -n \"" + sess + ":start_loadEnv:\" > " + time_log_fpath + "; ")
			sort_sbatch_file.write("date +%s >> " + time_log_fpath + "\n\n")

			sort_sbatch_file.write("source " + paths.MS_env_source + "\n\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_loadEnv:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: nsx2mda
			#################################

			sub_cmd_fpath = write_nsx2mda(session_dir, nsx_fpath, refset_jacksheet_fpath, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#nsx2mda\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#nsx2mda\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_nsx2mda:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_nsx2mda:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: reref_raw
			#################################

			sub_cmd_fpath = write_reref_raw(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#reref_raw\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#reref_raw\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_reref_raw:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_reref_raw:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: bandpass_raw
			#################################

			sub_cmd_fpath = write_bandpass_raw(session_dir, refset)

			# add sub_cmd to combu_run file

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#bandpass_raw\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#bandpass_raw\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_bandpass_raw:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_bandpass_raw:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: split_raw
			#################################

			sub_cmd_fpath = write_split_raw(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#split_raw\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#split_raw\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_split_raw:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_split_raw:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: reref_spike
			#################################

			sub_cmd_fpath = write_reref_spike(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#reref_spike\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#reref_spike\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_reref_spike:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_reref_spike:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: bandpass_spikeband
			#################################

			sub_cmd_fpath = write_bandpass_spike(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#bandpass_spike\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#bandpass_spike\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_bandpass_spike:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_bandpass_spike:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: split_spike
			#################################

			sub_cmd_fpath = write_split_spike(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#split_spike\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#split_spike\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_split_spike:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_split_spike:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: whiten_sort
			#################################

			sub_cmd_fpath = write_whiten_sort(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#whiten_sort\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#whiten_sort\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_whiten_sort:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_whiten_sort:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: split_sort
			#################################

			sub_cmd_fpath = write_split_sort(session_dir, refset)

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#split_sort\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#split_sort\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_split_sort:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_split_sort:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: sort
			#################################

			sub_cmd_fname = "sort%s.sh" % str(refset)
			sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
			sub_cmd_file = open(sub_cmd_fpath, 'w')

			sub_cmd_file.write("#!/bin/bash\n\n")
			sub_cmd_file.write("python3 " + paths.spikes_pipeline_dir + "/construct_split_sort_scripts.py " + session_dir + " " + str(refset) + "\n")
			sub_cmd_file.write("bash " + session_dir + "/sort_swarm%s.sh\n" % str(refset))

			sub_cmd_file.close()

			sort_sbatch_file.write("################################\n")
			sort_sbatch_file.write("#sort\n")
			sort_sbatch_file.write("################################\n")

			sort_sbatch_file.write("echo \"################################\"\n")
			sort_sbatch_file.write("echo \"#sort\"\n")
			sort_sbatch_file.write("echo \"################################\"\n")

			sort_sbatch_file.write("start_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
			sort_sbatch_file.write("echo \"" + sess + ":start_sort:$start_time\" >> " + time_log_fpath + ";\n\n")

			sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

			sort_sbatch_file.write("done_time=$(date +%s)\n")
			sort_sbatch_file.write("echo \"" + sess + ":done_sort:$done_time\" >> " + time_log_fpath + ";\n\n")

			#################################
			#################################
			# write sub-command: make spikeInfo
			#################################

			if write_spikeInfo_flag is True:
				write_spikeInfo(session_dir, combined_jacksheet_fpath, ns3_glob, nev_glob, partition, spikeInfo_mem)
				write_spikeInfo_flag = False

			sort_sbatch_file.close()

	return(command_tuple_list)

#########################################################################
# END FUNCTIONS #########################################################
#########################################################################


if __name__ == "__main__":

	# parse args
	parser = argparse.ArgumentParser(description='create bash files for spike sorting and lfp extraction')

	parser.add_argument('subj_path')
	parser.add_argument('--sesslist', default="")
	parser.add_argument('--output_suffix', default="initial")
	parser.add_argument('--keep_splits', action='store_false')
	parser.add_argument('--fresh_write', action='store_true')

	args = parser.parse_args()

	subj_path = args.subj_path
	sesslist_fname = args.sesslist
	output_suffix = args.output_suffix
	delete_splits = args.keep_splits
	fresh_write = args.fresh_write

	# convert min_range_cutoff_microvolt to millivolt
	min_range_cutoff_ungained = 10
	min_range_cutoff_millivolt = min_range_cutoff_ungained * 0.25 * (1/1000)

	# set time cutoff
	min_duration_minutes = 5

	# get session list
	if sesslist_fname != "":

		sesslist = open(sesslist_fname)
		subj_path_files = [l.strip("\n") for l in sesslist]
		sesslist.close()

	else:

		subj_path_files = os.listdir(subj_path)

	subj_path_files.sort()

	print("sessions: " + str(len(subj_path_files)))

	# gather all session bash lines in list
	sort_big_bash_list = []
	sort_big_bash_large_list = []
	sort_big_bash_xlarge_list = []

	session_count = 0

	for sess in subj_path_files:

		print("")
		print("looking at session " + sess, end="")

		if os.path.isdir(subj_path + "/" + sess) is True:

			print(" ... is a directory", end="")

			# read the session info file, if there is one
			session_jacksheet_glob = glob.glob(subj_path + "/" + sess + "/jacksheetBR_complete.csv")
			session_info_glob = glob.glob(subj_path + "/" + sess + "/*_info.txt")

			if session_info_glob != [] and session_jacksheet_glob != []:

				print(" ... has jacksheet + info", end="")

				session_info_fpath = session_info_glob[0]
				sesion_jacksheet_fpath = session_jacksheet_glob[0]

				# open the info.txt for this session
				session_info_file = open(session_info_fpath)
				session_info = [l.strip("\n") for l in session_info_file]
				session_info_file.close()

				nsx_ext = session_info[0]
				analog_pulse_ext = session_info[1]
				nsp_suffix = session_info[2]

				session_nsx_glob = glob.glob(subj_path + "/" + sess + "/*." + nsx_ext)

				# there is a nsx file, a jacksheet, and an info file. good to go
				if session_nsx_glob != []:

					print(" ... has an nsx file! zoom!", end="")

					session_nsx_fpath = session_nsx_glob[0]

					# write session scripts
					command_tuple_list = write_session_scripts(subj_path, sess, session_nsx_fpath, sesion_jacksheet_fpath, analog_pulse_ext, nsp_suffix, min_range_cutoff_millivolt, min_duration_minutes, delete_splits, fresh_write)
					print(command_tuple_list, end = "")
					session_count = session_count + 1

					for tup in command_tuple_list:

						size_cat = tup[0]
						bash_command = tup[1]

						if size_cat == "small":
							sort_big_bash_list.append(bash_command)
						elif size_cat == "large":
							sort_big_bash_large_list.append(bash_command)
						elif size_cat == "xlarge":
							sort_big_bash_xlarge_list.append(bash_command)

	print("")
	############################################################################################

	sort_big_bash_list += sort_big_bash_large_list
	sort_big_bash_large_list = []

	user_cpu_limit = 500
	total_big_bash_num = len(sort_big_bash_list + sort_big_bash_large_list + sort_big_bash_xlarge_list)

	big_bash_perc_cpu = math.floor(len(sort_big_bash_list)/float(total_big_bash_num) * user_cpu_limit)
	big_bash_large_perc_cpu = math.floor(len(sort_big_bash_large_list)/float(total_big_bash_num) * user_cpu_limit)
	big_bash_xlarge_perc_cpu = math.floor(len(sort_big_bash_xlarge_list)/float(total_big_bash_num) * user_cpu_limit)

	sort_big_bash_fname = "sort_%s_big_bash.sh" % output_suffix
	sort_big_bash_large_fname = "sort_%s_big_bash_large.sh" % output_suffix
	sort_big_bash_xlarge_fname = "sort_%s_big_bash_xlarge.sh" % output_suffix
	sort_swarm_fname = "sort_%s_swarm.sh" % output_suffix

	swarm_cpu_count = 10

	sort_swarm_command = "swarm -g 220 -b %s -t " + str(swarm_cpu_count) + " --time 15:00:00 --gres=lscratch:300 --merge-output --logdir "
	sort_large_swarm_command = "swarm -g 400 -b %s -t " + str(swarm_cpu_count) + " --partition largemem --time 15:00:00 --gres=lscratch:600 --merge-output --logdir "
	sort_xlarge_swarm_command = "swarm -g 700 -b %s -t " + str(swarm_cpu_count) + " --partition largemem --time 15:00:00 --gres=lscratch:600 --merge-output --logdir "

	# make subj_path/run_files if it doesnt exist, bash scripts go in there
	swarm_files_path = subj_path + "/_swarms"

	if os.path.isdir(swarm_files_path) is False:
		os.mkdir(swarm_files_path)
		os.mkdir(swarm_files_path + "/log_dump")

	sort_swarm_command += swarm_files_path + "/log_dump"
	sort_swarm_command += " -f "

	sort_large_swarm_command += swarm_files_path + "/log_dump"
	sort_large_swarm_command += " -f "

	sort_xlarge_swarm_command += swarm_files_path + "/log_dump"
	sort_xlarge_swarm_command += " -f "

	print("session_count with nsx file: " + str(session_count))

	# write sort swarm file
	swarm_file = open(swarm_files_path + "/" + sort_swarm_fname, 'w')

	if sort_big_bash_list != []:

		big_bash_target_num_bundle_groups = big_bash_perc_cpu/float(swarm_cpu_count)
		big_bash_bundle_size = math.ceil(len(sort_big_bash_list) / big_bash_target_num_bundle_groups)
		sort_swarm_command = sort_swarm_command % str(big_bash_bundle_size)

		print("sessions in sort_big_bash_list: " + str(len(sort_big_bash_list)))
		print("         resource distribution: " + str(big_bash_perc_cpu) + " cpus of " + str(user_cpu_limit) + " ~ bundle groups: " + str(big_bash_target_num_bundle_groups) + " with jobs per bundle: " + str(big_bash_bundle_size))

		sort_big_bash_file = open(swarm_files_path + "/" + sort_big_bash_fname, 'w')

		for l in sort_big_bash_list:
			sort_big_bash_file.write(l + "\n")

		sort_big_bash_file.close()

		swarm_file.write(sort_swarm_command + " " + swarm_files_path + "/" + sort_big_bash_fname + "\n")

	if sort_big_bash_large_list != []:

		big_bash_large_target_num_bundle_groups = big_bash_large_perc_cpu/float(swarm_cpu_count)
		big_bash_large_bundle_size = math.ceil(len(sort_big_bash_large_list) / big_bash_large_target_num_bundle_groups)
		sort_large_swarm_command = sort_large_swarm_command % str(big_bash_large_bundle_size)

		print("sessions in sort_big_bash_large_list: " + str(len(sort_big_bash_large_list)))
		print("               resource distribution: " + str(big_bash_large_perc_cpu) + " cpus of " + str(user_cpu_limit) + " ~ bundle groups: " + str(big_bash_large_target_num_bundle_groups) + " with jobs per bundle: " + str(big_bash_large_bundle_size))

		sort_big_bash_large_file = open(swarm_files_path + "/" + sort_big_bash_large_fname, 'w')

		for l in sort_big_bash_large_list:
			sort_big_bash_large_file.write(l + "\n")

		sort_big_bash_large_file.close()

		swarm_file.write(sort_large_swarm_command + " " + swarm_files_path + "/" + sort_big_bash_large_fname + "\n")

	if sort_big_bash_xlarge_list != []:

		big_bash_xlarge_target_num_bundle_groups = big_bash_xlarge_perc_cpu/float(swarm_cpu_count)
		big_bash_xlarge_bundle_size = math.ceil(len(sort_big_bash_xlarge_list) / big_bash_xlarge_target_num_bundle_groups)
		sort_xlarge_swarm_command = sort_xlarge_swarm_command % str(big_bash_xlarge_bundle_size)

		print("sessions in sort_big_bash_xlarge_list: " + str(len(sort_big_bash_xlarge_list)))
		print("                resource distribution: " + str(big_bash_xlarge_perc_cpu) + " cpus of " + str(user_cpu_limit) + " ~ bundle groups: " + str(big_bash_xlarge_target_num_bundle_groups) + " with jobs per bundle: " + str(big_bash_xlarge_bundle_size))

		sort_big_bash_xlarge_file = open(swarm_files_path + "/" + sort_big_bash_xlarge_fname, 'w')

		for l in sort_big_bash_xlarge_list:
			sort_big_bash_xlarge_file.write(l + "\n")

		sort_big_bash_xlarge_file.close()

		swarm_file.write(sort_xlarge_swarm_command + " " + swarm_files_path + "/" + sort_big_bash_xlarge_fname + "\n")

	swarm_file.close()
