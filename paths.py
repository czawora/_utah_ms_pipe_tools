
data_dir_name = "FRNU"

matlab_compiler_ver_str = "/lscratch/$SLURM_JOB_ID/v94"

spikes_pipeline_dir = "/data/%s/python_scripts/_utah_ms_pipe_tools" % data_dir_name
lfp_pipeline_dir = "/data/%s/python_scripts/_utah_lfp_pipe_tools" % data_dir_name

mountainsort_binaries_dir = "/data/%s/installs/mountainlab/bin" % data_dir_name
nsx2mda_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
splitmda_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
globalReref_allchan_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
construct_spikeInfoMS_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
plotChannelSpikes_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
MS_env_source = "/data/%s/mod_sets/ms.sh" % data_dir_name

splitLFP_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
variance_exclusion_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
filter_matlab_dir = "/data/%s/matlab_scripts/_utah_matlab_tools" % data_dir_name
