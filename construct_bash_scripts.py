
import os
import glob
import time
import argparse
import math
import sys
import re
import shutil
import csv

#########################################################################
#########################################################################
#########################################################################

datestring_regex_old = re.compile(r'.*(\d\d\d\d\d\d_\d\d\d\d).*')
datestring_regex_new = re.compile(r'.*(\d\d\d\d\d\d\d\d-\d\d\d\d\d\d).*')


abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

sys.path.append(dname)

if os.path.isfile(dname + "/paths.py") is False:
    print("move a copy of paths.py into this folder: " + dname)
    exit(2)

from paths import *


bash_fname = "sort_run_all%s.sh"
bash_log_fname = "_sort_run_all%s.log"

#########################################################################
#########################################################################
#########################################################################

parser = argparse.ArgumentParser(description='create bash files for spike sorting and lfp extraction')

parser.add_argument('subj_path')
parser.add_argument('--sesslist', default = "")
parser.add_argument('--output_suffix', default = "initial")
parser.add_argument('--keep_splits', action='store_false')

args = parser.parse_args()

subj_path = args.subj_path
sesslist_fname = args.sesslist
output_suffix = args.output_suffix
delete_splits = args.keep_splits

timestamp = time.strftime("%d_%m_%Y--%H_%M_%S")

log_files = []

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

    print(sess)
    # if the sess is raw blackrock file string, need to change it
    if re.match(datestring_regex_new, sess) is not None:

        datestring_match = re.findall(datestring_regex_new, sess)[0]
        #print("found datestring match: " + datestring_match)
        datestring_match_splitter = datestring_match
        #print("split on datestring match: " + str(sess.split(datestring_match_splitter)))

        output_subj_str = sess.split(datestring_match_splitter)[0].strip("_")
        output_ymd_hms_str = datestring_match[2:len(datestring_match)-2].replace('-', "_")
        output_nsp_str = sess.split(datestring_match_splitter)[1]

        if output_nsp_str[0] == "-":
            output_nsp_str = output_nsp_str[1:]

    elif re.match(datestring_regex_old, sess) is not None:

        datestring_match = re.findall(datestring_regex_old, sess)[0]
        print("found datestring match: " + datestring_match)
        datestring_match_splitter = datestring_match
        print("split on datestring match: " + str(sess.split(datestring_match_splitter)))

        output_subj_str = sess.split(datestring_match_splitter)[0].strip("_")
        output_ymd_hms_str = datestring_match
        output_nsp_str = sess.split(datestring_match_splitter)[1]

        if output_nsp_str[0] == "_":
            output_nsp_str = output_nsp_str[1:]

    print("looking at session " + sess + " ymd_hms --> " + output_ymd_hms_str)

    if os.path.isdir(subj_path + "/" + sess) is True:

        print(subj_path + "/" + sess + " is a directory")

        # read the session info file, if there is one
        session_info_glob = glob.glob(subj_path + "/" + sess + "/*_info.txt")
        session_elementInfo_glob = glob.glob(subj_path + "/" + sess + "/*_elementInfo.txt")

        if session_info_glob != [] and session_elementInfo_glob != []:

            session_elementInfo_fpath = session_elementInfo_glob[0]

            session_elementInfo_file = open(session_elementInfo_fpath)
            session_elementInfo_filecsv = csv.reader(session_elementInfo_file, delimiter=',', quotechar='"')
            session_elementInfo = [l for l in session_elementInfo_filecsv]
            session_elementInfo_file.close()

            # get number of refsets
            session_elementInfo_refsets = len(session_elementInfo)
            print("num refsets: " + str(session_elementInfo_refsets))

            session_info_file = open(session_info_glob[0])
            session_info = [l.strip("\n") for l in session_info_file]
            session_info_file.close()

            analog_pulse_ext = session_info[0]
            nsx_ext = session_info[1]

            # find the nsx file in this session
            g = glob.glob(subj_path + "/" + sess + "/*." + nsx_ext)

            if g != []:

                session_count = session_count + 1

                session_dir = subj_path + "/" + sess + "/spike"
                if os.path.isdir(session_dir) is False:
                    os.mkdir(session_dir)

                job_name = "FRNU--" + timestamp + "--" + sess

                nsx_file = g[0]
                nsx_filesize = os.path.getsize(nsx_file)

                nsx_fname = nsx_file.split("/")[-1]

                ns3_glob = glob.glob(subj_path + "/" + sess + "/*." + analog_pulse_ext)
                nev_glob = glob.glob(subj_path + "/" + sess + "/*.nev")

                for iRefset in range(1, session_elementInfo_refsets + 1):

                    print(bash_fname)

                    current_bash_fname = bash_fname % str(iRefset)
                    current_bash_log_fname = bash_log_fname % str(iRefset)

                    print(current_bash_fname)

                    if nsx_filesize/1e9 < 25:
                        sort_big_bash_list.append("bash " + session_dir + "/" + current_bash_fname + "\n")
                    elif nsx_filesize/1e9 < 40:
                        sort_big_bash_large_list.append("bash " + session_dir + "/" + current_bash_fname + "\n")
                    else:
                        sort_big_bash_xlarge_list.append("bash " + session_dir + "/" + current_bash_fname + "\n")

                    if delete_splits and os.path.isdir(session_dir + "/splits"):
                        shutil.rmtree(session_dir + "/splits")

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
                    sbatch_header.append("#SBATCH --gres=lscratch:1")

                    for l in sbatch_header:
                        sort_sbatch_file.write(l + "\n")

                    sort_sbatch_file.write("\n\n")

                    # remove an existing _ignore_me.txt
                    sort_sbatch_file.write("if [ -f " + session_dir + "/_ignore_me%s.txt ]; then\n" % str(iRefset))
                    sort_sbatch_file.write("rm " + session_dir + "/_ignore_me%s.txt\n" % str(iRefset))
                    sort_sbatch_file.write("fi\n\n")

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

                    sort_sbatch_file.write("source " + MS_env_source + "\n\n")

                    sort_sbatch_file.write("done_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"" + sess + ":done_loadEnv:$done_time\" >> " + time_log_fpath + ";\n\n")

                    #################################
                    #################################
                    #################################
                    #################################

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: convert2mda
                    #################################

                    sub_cmd_fname = "nsx2mda%s.sh" % str(iRefset)
                    sub_cmd_log_fname = "_nsx2mda%s.log" % str(iRefset)
                    sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
                    sub_cmd_file = open(sub_cmd_fpath, 'w')

                    # write the sbatch header for sub_cmd bash file

                    sbatch_header = []
                    sbatch_header.append("#!/bin/bash")
                    sbatch_header.append("#SBATCH --mem=120g")
                    sbatch_header.append("#SBATCH --cpus-per-task=2")
                    sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
                    sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
                    sbatch_header.append("#SBATCH --gres=lscratch:1")

                    for l in sbatch_header:
                        sub_cmd_file.write(l + "\n")

                    sub_cmd_file.write("\n\n")

                    sub_cmd_file.write("echo \"start nsx2mda\"\n")
                    sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

                    # convert to mda format
                    matlab_command = "cd " + nsx2mda_matlab_dir + "/_nsx2mda; ./run_nsx2mda_swarm.sh " + matlab_compiler_ver_str

                    sub_cmd = []
                    sub_cmd.append(matlab_command)
                    sub_cmd.append("input_filename")
                    sub_cmd.append(nsx_file)
                    sub_cmd.append("output_dir")
                    sub_cmd.append(session_dir)
                    sub_cmd.append("refset")
                    sub_cmd.append(str(iRefset))
                    sub_cmd.append("elementInfo_filename")
                    sub_cmd.append(session_elementInfo_fpath)
                    sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                    sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                    sub_cmd_file.write("echo \"end nsx2mda\"\n")

                    sub_cmd_file.close()

                    log_files.append(session_dir + "/" + sub_cmd_log_fname)

                    #################################
                    #################################
                    #################################
                    #################################

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

                    sort_sbatch_file.write("cat " + session_dir + "/" + sub_cmd_log_fname + " > " + session_dir + "/" + current_bash_log_fname + "\n\n")

                    sort_sbatch_file.write("################################\n")
                    sort_sbatch_file.write("#make check if ignore file is present\n")
                    sort_sbatch_file.write("################################\n\n")

                    sort_sbatch_file.write("if [ ! -f " + session_dir + "/_ignore_me%s.txt ]; then\n\n" % str(iRefset))
                    print(session_dir + "/" + current_bash_fname)

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: bandpass
                    #################################

                    # find the output mda file from above

                    sub_cmd_fname = "bandpass%s.sh" % str(iRefset)
                    sub_cmd_log_fname = "_bandpass%s.log" % str(iRefset)
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

                    sub_cmd_file.write("source " + MS_env_source + "\n\n")

                    sub_cmd_file.write("echo \"start bandpass\"\n")
                    sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

                    sub_cmd_file.write("bandpass_input_nsx=`ls " + session_dir + "/* | grep \"refset%s\.mda$\"`\n" % str(iRefset))
                    sub_cmd_file.write("bandpass_output_mda=${bandpass_input_nsx}_bp\n\n")

                    sub_cmd_file.write("echo \"input:${bandpass_input_nsx}\"\n")
                    sub_cmd_file.write("echo \"output:${bandpass_output_mda}\"\n\n")

                    sub_cmd_file.write("if [ -f $bandpass_output_mda  ]\n")
                    sub_cmd_file.write("then\n")
                    sub_cmd_file.write("\trm $bandpass_output_mda \n")
                    sub_cmd_file.write("fi\n\n")

                    sub_cmd = []
                    sub_cmd.append(mountainsort_binaries_dir + "/mp-run-process")
                    sub_cmd.append("ms3.bandpass_filter")
                    sub_cmd.append("--timeseries=${bandpass_input_nsx}")
                    sub_cmd.append("--timeseries_out=${bandpass_output_mda}")
                    sub_cmd.append("--samplerate=30000")
                    sub_cmd.append("--freq_min=600")
                    sub_cmd.append("--freq_max=6000")

                    sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                    sub_cmd_file.write("for i in `seq 1 5`;\n")
                    sub_cmd_file.write("do\n")
                    sub_cmd_file.write("if [ ! -f ${bandpass_output_mda} ]; then\n")

                    sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                    sub_cmd_file.write("fi\n")
                    sub_cmd_file.write("done\n")

                    sub_cmd_file.write("echo \"end bandpass\"\n")

                    sub_cmd_file.close()

                    log_files.append(session_dir + "/" + sub_cmd_log_fname)

                    #################################
                    #################################
                    #################################
                    #################################

                    # add sub_cmd to combu_run file

                    sort_sbatch_file.write("################################\n")
                    sort_sbatch_file.write("#bandpass\n")
                    sort_sbatch_file.write("################################\n")

                    sort_sbatch_file.write("echo \"################################\"\n")
                    sort_sbatch_file.write("echo \"#bandpass\"\n")
                    sort_sbatch_file.write("echo \"################################\"\n")

                    sort_sbatch_file.write("start_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
                    sort_sbatch_file.write("echo \"" + sess + ":start_bandpass:$start_time\" >> " + time_log_fpath + ";\n\n")

                    sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

                    sort_sbatch_file.write("done_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"" + sess + ":done_bandpass:$done_time\" >> " + time_log_fpath + ";\n\n")
                    sort_sbatch_file.write("cat " + session_dir + "/" + sub_cmd_log_fname + " >> " + session_dir + "/" + current_bash_log_fname + "\n\n")

                    sort_sbatch_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda$\"`\n\n" % str(iRefset))

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: reref
                    #################################

                    sub_cmd_fname = "reref%s.sh" % str(iRefset)
                    sub_cmd_log_fname = "_reref%s.log" % str(iRefset)
                    sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
                    sub_cmd_file = open(sub_cmd_fpath, 'w')

                    # write the sbatch header for sub_cmd bash file
                    sbatch_header = []
                    sbatch_header.append("#!/bin/bash")
                    sbatch_header.append("#SBATCH --mem=120g")
                    sbatch_header.append("#SBATCH --cpus-per-task=10")
                    sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
                    sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
                    sbatch_header.append("#SBATCH --gres=lscratch:1")

                    for l in sbatch_header:
                        sub_cmd_file.write(l + "\n")

                    sub_cmd_file.write("\n\n")

                    sub_cmd_file.write("echo \"start reref\"\n")
                    sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

                    sub_cmd_file.write("used_chans_fpath=`ls " + session_dir + "/* | grep \"refset%s_used_chans\.txt$\"`\n" % str(iRefset))
                    sub_cmd_file.write("bandpass_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda_bp$\"`\n" % str(iRefset))
                    sub_cmd_file.write("reref_output_mda=${bandpass_input_mda}_reref\n\n")

                    sub_cmd_file.write("echo \"input:${bandpass_input_mda}\"\n")
                    sub_cmd_file.write("echo \"output:${reref_output_mda}\"\n\n")

                    matlab_command = "cd " + globalReref_allchan_matlab_dir + "/_globalReref_allchan; ./run_globalReref_allchan_swarm.sh " + matlab_compiler_ver_str

                    sub_cmd = []
                    sub_cmd.append(matlab_command)
                    sub_cmd.append("$bandpass_input_mda")
                    sub_cmd.append("$reref_output_mda")
                    sub_cmd.append(str(iRefset))
                    sub_cmd.append("$used_chans_fpath")

                    sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                    sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                    sub_cmd_file.write("echo \"end reref\"\n")

                    sub_cmd_file.close()

                    log_files.append(session_dir + "/" + sub_cmd_log_fname)

                    #################################
                    #################################
                    #################################
                    #################################

                    # add sub_cmd to combo_run file

                    sort_sbatch_file.write("################################\n")
                    sort_sbatch_file.write("#reref\n")
                    sort_sbatch_file.write("################################\n")

                    sort_sbatch_file.write("echo \"################################\"\n")
                    sort_sbatch_file.write("echo \"#reref\"\n")
                    sort_sbatch_file.write("echo \"################################\"\n")

                    sort_sbatch_file.write("start_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
                    sort_sbatch_file.write("echo \"" + sess + ":start_reref:$start_time\" >> " + time_log_fpath + ";\n\n")

                    sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

                    sort_sbatch_file.write("done_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"" + sess + ":done_reref:$done_time\" >> " + time_log_fpath + ";\n\n")
                    sort_sbatch_file.write("cat " + session_dir + "/" + sub_cmd_log_fname + " >> " + session_dir + "/" + current_bash_log_fname + "\n\n")

                    sort_sbatch_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_bp$\"`\n\n" % str(iRefset))

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: whiten
                    #################################

                    # find the output mda file from above

                    sub_cmd_fname = "whiten%s.sh" % str(iRefset)
                    sub_cmd_log_fname = "_whiten%s.log" % str(iRefset)
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

                    sub_cmd_file.write("source " + MS_env_source + "\n\n")

                    sub_cmd_file.write("echo \"start whiten\"\n")
                    sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

                    sub_cmd_file.write("whiten_input=`ls " + session_dir + "/* | grep \"refset%s\.mda_bp_reref$\"`\n" % str(iRefset))
                    sub_cmd_file.write("whiten_output_mda=${whiten_input}_whiten\n\n")

                    sub_cmd_file.write("echo \"input:${whiten_input}\"\n")
                    sub_cmd_file.write("echo \"output:${whiten_output_mda}\"\n\n")

                    sub_cmd_file.write("if [ -f $whiten_output_mda  ]\n")
                    sub_cmd_file.write("then\n")
                    sub_cmd_file.write("\trm $whiten_output_mda \n")
                    sub_cmd_file.write("fi\n\n")

                    sub_cmd = []
                    sub_cmd.append(mountainsort_binaries_dir + "/mp-run-process")
                    sub_cmd.append("ms3.whiten")
                    sub_cmd.append("--timeseries=${whiten_input}")
                    sub_cmd.append("--timeseries_out=${whiten_output_mda}")

                    sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                    sub_cmd_file.write("for i in `seq 1 5`;\n")
                    sub_cmd_file.write("do\n")
                    sub_cmd_file.write("if [ ! -f ${whiten_output_mda} ]; then\n")

                    sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                    sub_cmd_file.write("fi\n")
                    sub_cmd_file.write("done\n")

                    sub_cmd_file.write("echo \"end bandpass\"\n")

                    sub_cmd_file.close()

                    log_files.append(session_dir + "/" + sub_cmd_log_fname)

                    #################################
                    #################################
                    #################################
                    #################################

                    # add sub_cmd to combu_run file

                    sort_sbatch_file.write("################################\n")
                    sort_sbatch_file.write("#whiten\n")
                    sort_sbatch_file.write("################################\n")

                    sort_sbatch_file.write("echo \"################################\"\n")
                    sort_sbatch_file.write("echo \"#whiten\"\n")
                    sort_sbatch_file.write("echo \"################################\"\n")

                    sort_sbatch_file.write("start_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
                    sort_sbatch_file.write("echo \"" + sess + ":start_whiten:$start_time\" >> " + time_log_fpath + ";\n\n")

                    sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

                    sort_sbatch_file.write("done_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"" + sess + ":done_whiten:$done_time\" >> " + time_log_fpath + ";\n\n")
                    sort_sbatch_file.write("cat " + session_dir + "/" + sub_cmd_log_fname + " >> " + session_dir + "/" + current_bash_log_fname + "\n\n")

                    sort_sbatch_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_bp_reref$\"`\n\n" % str(iRefset))

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: split
                    #################################

                    # write the sort file
                    sub_cmd_fname = "split%s.sh" % str(iRefset)
                    sub_cmd_log_fname = "_split%s.log" % str(iRefset)
                    sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
                    sub_cmd_file = open(sub_cmd_fpath, 'w')

                    # write the sbatch header for sub_cmd bash file

                    sbatch_header = []
                    sbatch_header.append("#!/bin/bash")
                    sbatch_header.append("#SBATCH --mem=120g")
                    sbatch_header.append("#SBATCH --cpus-per-task=10")
                    sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
                    sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)
                    sbatch_header.append("#SBATCH --gres=lscratch:1")

                    for l in sbatch_header:
                        sub_cmd_file.write(l + "\n")

                    sub_cmd_file.write("\n\n")

                    sub_cmd_file.write("reref_input_mda=`ls " + session_dir + "/* | grep \"refset%s\.mda_bp_reref_whiten$\"`\n" % str(iRefset))
                    sub_cmd_file.write("used_chans_fpath=`ls " + session_dir + "/* | grep \"refset%s_used_chans\.txt$\"`\n" % str(iRefset))
                    sub_cmd_file.write("echo \"input:${reref_input_mda}\"\n")
                    sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

                    split_dir = session_dir + "/splits"

                    sub_cmd_file.write("if [ ! -d \"" + split_dir + "\" ]; then\n")
                    sub_cmd_file.write("mkdir " + split_dir + "\n")
                    sub_cmd_file.write("fi\n")

                    matlab_command = "cd " + splitmda_matlab_dir + "/_splitmda; ./run_splitmda_swarm.sh " + matlab_compiler_ver_str

                    sub_cmd = []
                    sub_cmd.append(matlab_command)
                    sub_cmd.append("input_filename")
                    sub_cmd.append("$reref_input_mda")
                    sub_cmd.append("output_dir")
                    sub_cmd.append(split_dir)
                    sub_cmd.append("used_chans_fpath")
                    sub_cmd.append("$used_chans_fpath")
                    sub_cmd.append("refset")
                    sub_cmd.append(str(iRefset))

                    sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                    sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                    sub_cmd_file.close()

                    log_files.append(session_dir + "/" + sub_cmd_log_fname)

                    #################################
                    #################################
                    #################################
                    #################################

                    # add sub_cmd to combo_run file

                    sort_sbatch_file.write("################################\n")
                    sort_sbatch_file.write("#split\n")
                    sort_sbatch_file.write("################################\n")

                    sort_sbatch_file.write("echo \"################################\"\n")
                    sort_sbatch_file.write("echo \"#split\"\n")
                    sort_sbatch_file.write("echo \"################################\"\n")

                    sort_sbatch_file.write("start_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
                    sort_sbatch_file.write("echo \"" + sess + ":start_split:$start_time\" >> " + time_log_fpath + ";\n\n")

                    sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

                    sort_sbatch_file.write("done_time=$(date +%s)\n")
                    sort_sbatch_file.write("echo \"" + sess + ":done_split:$done_time\" >> " + time_log_fpath + ";\n\n")
                    sort_sbatch_file.write("cat " + session_dir + "/" + sub_cmd_log_fname + " >> " + session_dir + "/" + current_bash_log_fname + "\n\n")

                    sort_sbatch_file.write("rm `ls " + session_dir + "/* | grep \"refset%s\.mda_bp_reref_whiten$\"`\n\n" % str(iRefset))

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: sort
                    #################################

                    sub_cmd_fname = "sort%s.sh" % str(iRefset)
                    sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
                    sub_cmd_file = open(sub_cmd_fpath, 'w')

                    sub_cmd_file.write("#!/bin/bash\n\n")
                    sub_cmd_file.write("python3 " + spikes_pipeline_dir + "/construct_split_sort_scripts.py " + session_dir + " " + job_name + " " + str(iRefset) + "\n")
                    sub_cmd_file.write("bash " + session_dir + "/sort_swarm%s.sh\n" % str(iRefset))

                    sub_cmd_file.close()

                    sort_sbatch_file.write("################################\n")
                    sort_sbatch_file.write("#sort\n")
                    sort_sbatch_file.write("################################\n")

                    sort_sbatch_file.write("echo \"################################\"\n")
                    sort_sbatch_file.write("echo \"#sort\"\n")
                    sort_sbatch_file.write("echo \"################################\"\n")

                    sort_sbatch_file.write("start_time=$(date +%s)\n")
                    sort_sbatch_file.write(
                        "echo \"#$((start_time - done_time))\" >> " + time_log_fpath + "\n")
                    sort_sbatch_file.write(
                        "echo \"" + sess + ":start_sort:$start_time\" >> " + time_log_fpath + ";\n\n")

                    sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

                    sort_sbatch_file.write("done_time=$(date +%s)\n")
                    sort_sbatch_file.write(
                        "echo \"" + sess + ":done_sort:$done_time\" >> " + time_log_fpath + ";\n\n")

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    #################################
                    #################################
                    # write sub-command: make spikeInfo
                    #################################

                    if iRefset == 1:

                        split_dir = session_dir + "/splits"

                        spikeInfo_fpath = session_dir + "/outputs/" + "_".join([output_subj_str, output_ymd_hms_str, output_nsp_str]) + "_spikeInfo.mat"
                        summary_fpath = session_dir + "/outputs/" + "_".join([output_subj_str, output_ymd_hms_str, output_nsp_str]) + "_sortSummary.csv"

                        # write the sort file
                        sub_cmd_fname = "spikeInfo.sh"
                        sub_cmd_log_fname = "_spikeInfo.log"
                        sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
                        sub_cmd_file = open(sub_cmd_fpath, 'w')

                        # write the sbatch header for sub_cmd bash file
                        sbatch_header = []
                        sbatch_header.append("#!/bin/bash")
                        sbatch_header.append("#SBATCH --mem=10g")
                        sbatch_header.append("#SBATCH --cpus-per-task=1")
                        sbatch_header.append("#SBATCH --error=" + session_dir + "/" + sub_cmd_log_fname)
                        sbatch_header.append("#SBATCH --output=" + session_dir + "/" + sub_cmd_log_fname)

                        for l in sbatch_header:
                            sub_cmd_file.write(l + "\n")

                        sub_cmd_file.write("#SBATCH --job-name=" + job_name + "\n")
                        sub_cmd_file.write("#SBATCH --dependency=singleton\n")
                        sub_cmd_file.write("#SBATCH --time=48:00:00\n")
                        sub_cmd_file.write("#SBATCH --gres=lscratch:1\n")

                        sub_cmd_file.write("\n\n")

                        sub_cmd_file.write("if [ -f \"" + spikeInfo_fpath + "\" ]; then\n")
                        sub_cmd_file.write("rm " + spikeInfo_fpath + "\n")
                        sub_cmd_file.write("fi\n\n")

                        sub_cmd_file.write("if [ -f \"" + summary_fpath + "\" ]; then\n")
                        sub_cmd_file.write("rm " + summary_fpath + "\n")
                        sub_cmd_file.write("fi\n\n")

                        sub_cmd_file.write("echo \"SLURM_JOB_ID = $SLURM_JOB_ID\" &> " + session_dir + "/" + sub_cmd_log_fname + "\n")

                        matlab_command = "cd " + construct_spikeInfoMS_matlab_dir + "/_construct_spikeInfoMS; ./run_construct_spikeInfoMS_swarm.sh " + matlab_compiler_ver_str

                        sub_cmd = []
                        sub_cmd.append(matlab_command)

                        sub_cmd.append("subj_str")
                        sub_cmd.append(output_subj_str)

                        sub_cmd.append("time_str")
                        sub_cmd.append(output_ymd_hms_str)

                        sub_cmd.append("nsp_str")
                        sub_cmd.append(output_nsp_str)

                        sub_cmd.append("sessRoot")
                        sub_cmd.append(split_dir)

                        sub_cmd.append("bp_fname_suffix")
                        sub_cmd.append("mda_chan")

                        sub_cmd.append("nsx_physio_fpath")
                        sub_cmd.append(nsx_file)

                        if ns3_glob != []:
                            sub_cmd.append("ns3_pulse_fpath")
                            sub_cmd.append(ns3_glob[0])  # ns3_fpath

                        if nev_glob != []:
                            sub_cmd.append("nev_fpath")
                            sub_cmd.append(nev_glob[0])  # nev_fpath

                        sub_cmd.append("saveRoot")
                        sub_cmd.append(session_dir + "/outputs")

                        sub_cmd.append("removeLargeAmpUnits")
                        sub_cmd.append("0")

                        sub_cmd.append("elementInfo_fpath")
                        sub_cmd.append(session_elementInfo_fpath)

                        sub_cmd.append("&> " + session_dir + "/" + sub_cmd_log_fname)

                        sub_cmd_file.write(" ".join(sub_cmd) + "\n")

                        sub_cmd_file.write("if [ -f \"" + spikeInfo_fpath + "\" ]; then\n")
                        sub_cmd_file.write("rm -r " + split_dir + "\n")
                        sub_cmd_file.write("fi\n\n")

                        sub_cmd_file.close()

                        # #################################
                        # #################################
                        # #################################
                        # #################################

                        # #add sub_cmd to combo_run file

                        #sort_sbatch_file.write("################################\n")
                        #sort_sbatch_file.write("#spikeInfo\n")
                        #sort_sbatch_file.write("################################\n")

                        #sort_sbatch_file.write("sbatch " + sub_cmd_fpath + "\n")

                    ###################################################################################################
                    ###################################################################################################
                    ###################################################################################################

                    # closing fi for check if _ignore_me.txt is present
                    sort_sbatch_file.write("fi\n\n")

                    sort_sbatch_file.close()

                #################################
                #################################
                # write sub-command: paste logs together
                #################################

                # sub_cmd_fname = "cat_logs.sh"
                # sub_cmd_fpath = session_dir + "/" + sub_cmd_fname
                # sub_cmd_file = open(sub_cmd_fpath, 'w')
                #
                # sub_cmd_file.write("cat " + " ".join(log_files) + " > " + session_dir + "/" + bash_log_fname)
                #
                # sub_cmd_file.close()

                #################################
                #################################
                #################################
                #################################

                # sort_sbatch_file.write("bash " + sub_cmd_fpath + "\n")

                ###################################################################################################
                ###################################################################################################
                ###################################################################################################

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

sort_swarm_command = "swarm -g 220 -b %s -t " + str(swarm_cpu_count) + " --time 5:00:00 --gres=lscratch:1 --merge-output --logdir "
sort_large_swarm_command = "swarm -g 400 -b %s -t " + str(swarm_cpu_count) + " --partition largemem --time 5:00:00 --gres=lscratch:1 --merge-output --logdir "
sort_xlarge_swarm_command = "swarm -g 700 -b %s -t " + str(swarm_cpu_count) + " --partition largemem --time 5:00:00 --gres=lscratch:1 --merge-output --logdir "

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

big_bash_target_num_bundle_groups = big_bash_perc_cpu/float(swarm_cpu_count)
big_bash_bundle_size = math.ceil(len(sort_big_bash_list) / big_bash_target_num_bundle_groups)
sort_swarm_command = sort_swarm_command % str(big_bash_bundle_size)

big_bash_large_target_num_bundle_groups = big_bash_large_perc_cpu/float(swarm_cpu_count)
big_bash_large_bundle_size = math.ceil(len(sort_big_bash_large_list) / big_bash_large_target_num_bundle_groups)
sort_large_swarm_command = sort_large_swarm_command % str(big_bash_large_bundle_size)

big_bash_xlarge_target_num_bundle_groups = big_bash_xlarge_perc_cpu/float(swarm_cpu_count)
big_bash_xlarge_bundle_size = math.ceil(len(sort_big_bash_xlarge_list) / big_bash_xlarge_target_num_bundle_groups)
sort_xlarge_swarm_command = sort_xlarge_swarm_command % str(big_bash_xlarge_bundle_size)

print("session_count with nsx file: " + str(session_count))

# write sort swarm file
swarm_file = open(swarm_files_path + "/" + sort_swarm_fname, 'w')

if sort_big_bash_list != []:

    print("sessions in sort_big_bash_list: " + str(len(sort_big_bash_list)))
    print("                              : " + str(big_bash_perc_cpu) + " cpus of " + str(user_cpu_limit) + " ~ bundle groups: " + str(big_bash_target_num_bundle_groups) + " with jobs per bundle: " + str(big_bash_bundle_size))

    sort_big_bash_file = open(swarm_files_path + "/" + sort_big_bash_fname, 'w')

    for l in sort_big_bash_list:
        sort_big_bash_file.write(l)

    sort_big_bash_file.close()

    swarm_file.write(sort_swarm_command + " " + swarm_files_path + "/" + sort_big_bash_fname + "\n")

if sort_big_bash_large_list != []:

    print("sessions in sort_big_bash_large_list: " + str(len(sort_big_bash_large_list)))
    print("                              : " + str(big_bash_large_perc_cpu) + " cpus of " + str(user_cpu_limit) + " ~ bundle groups: " + str(big_bash_large_target_num_bundle_groups) + " with jobs per bundle: " + str(big_bash_large_bundle_size))

    sort_big_bash_large_file = open(swarm_files_path + "/" + sort_big_bash_large_fname, 'w')

    for l in sort_big_bash_large_list:
        sort_big_bash_large_file.write(l)

    sort_big_bash_large_file.close()

    swarm_file.write(sort_large_swarm_command + " " + swarm_files_path + "/" + sort_big_bash_large_fname + "\n")

if sort_big_bash_xlarge_list != []:

    print("sessions in sort_big_bash_xlarge_list: " + str(len(sort_big_bash_xlarge_list)))
    print("                              : " + str(big_bash_xlarge_perc_cpu) + " cpus of " + str(user_cpu_limit) + " ~ bundle groups: " + str(big_bash_xlarge_target_num_bundle_groups) + " with jobs per bundle: " + str(big_bash_xlarge_bundle_size))

    sort_big_bash_xlarge_file = open(swarm_files_path + "/" + sort_big_bash_xlarge_fname, 'w')

    for l in sort_big_bash_xlarge_list:
        sort_big_bash_xlarge_file.write(l)

    sort_big_bash_xlarge_file.close()

    swarm_file.write(sort_xlarge_swarm_command + " " + swarm_files_path + "/" + sort_big_bash_xlarge_fname + "\n")


swarm_file.close()
