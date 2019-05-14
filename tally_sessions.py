
import os
import glob
import argparse
from subprocess import call

from paths import *

parser = argparse.ArgumentParser()

parser.add_argument('subj_path')
parser.add_argument('--rerun', action='store_true')

args = parser.parse_args()

subj_path = args.subj_path
rerun = args.rerun

sess_info_list = glob.glob(subj_path + "/*/*jacksheet*")
sess_list = ["/".join(iInfo.split("/")[:-1]) for iInfo in sess_info_list]

if len(sess_list) == 0:

    print("no sessions found in subj path: " + subj_path)
    exit(1)

ignore_strings = []
incomplete_strings = []
complete_strings = []

incomplete_chans = []
incomplete_outputs = []
incomplete_chans_sess = []
incomplete_outputs_sess = []

total_count = 0

ignore_stats = []
outputs_stats = []
spikeInfo_stats = []
spikeWaveform_stats = []
sortSummary_stats = []
sortFigs_stats = []
splits_chan_stats = []
splits_done_stats = []


for idx, sess in enumerate(sess_list):

    sess_name = sess.split("/")[-1]
    sess_path = sess + "/spike"
    sess_path_outputs = sess_path + "/outputs"
    sess_path_splits = sess_path + "/splits_sort"

    total_count += 1

    ignore_status = 0
    outputs_status = 0
    spikeInfo_status = 0
    spikeWaveform_status = 0
    sortSummary_status = 0
    sortFigs_status = 0
    traceFigs_status = 0
    splits_chan_status = 0
    splits_done_status = 0

    # is there a ignore_me.txt in this session
    if glob.glob(sess_path + "/ignore_me*") != []:

        ignore_status = len(glob.glob(sess_path + "/ignore_me*"))

    if os.path.isdir(sess_path_outputs):

        outputs_status = 1

        spikeInfo_glob = glob.glob(sess_path + "/outputs/*spikeInfo.mat")
        spikeWaveform_glob = glob.glob(sess_path + "/outputs/*spikeWaveform.mat")
        sortSummary_glob = glob.glob(sess_path + "/outputs/*sortSummary.csv")

        if spikeInfo_glob != []:
            spikeInfo_status = 1
        if spikeWaveform_glob != []:
            spikeWaveform_status = 1
        if sortSummary_glob != []:
            sortSummary_status = 1

        if os.path.isdir(sess_path + "/outputs/sortFigs"):

            sortFigs_glob = glob.glob(sess_path + "/outputs/sortFigs/*")
            sortFigs_status = len(sortFigs_glob)

    if os.path.isdir(sess_path_splits):

        splits_chan_status = len(glob.glob(sess_path_splits + "/*/*mda_chan"))
        splits_done_status = len(glob.glob(sess_path_splits + "/*/done.log"))

    ignore_stats.append(ignore_status)
    outputs_stats.append(outputs_status)
    spikeInfo_stats.append(spikeInfo_status)
    spikeWaveform_stats.append(spikeWaveform_status)
    sortSummary_stats.append(sortSummary_status)
    sortFigs_stats.append(sortFigs_status)
    splits_chan_stats.append(splits_chan_status)
    splits_done_stats.append(splits_done_status)


for idx, sess in enumerate(sess_list):

    sess_path = sess + "/spike"

    ignore_status = ignore_stats[idx]
    outputs_status = outputs_stats[idx]
    spikeInfo_status = spikeInfo_stats[idx]
    spikeWaveform_status = spikeWaveform_stats[idx]
    sortSummary_status = sortSummary_stats[idx]
    sortFigs_status = sortFigs_stats[idx]
    splits_chan_status = splits_chan_stats[idx]
    splits_done_status = splits_done_stats[idx]

    # completed session should be
    # ignore = 1
    # OR
    # ignore = 0
    # outputs = 1
    # spikeInfo = 1
    # spikeWaveform = 1
    # sortSummary = 1
    # sortFigs = not easily indicative
    # splits_chan == splits_done

    if ignore_status != 0:

        current_ignore_string = ""
        for ignore_fpath in glob.glob(sess_path + "/ignore_me*.txt"):

            ignore_fname = ignore_fpath.split("/")[-1]
            ignore_file = open(ignore_fpath)
            ignore_lines = [l.strip("\n") for l in ignore_file]
            ignore_file.close()

            current_ignore_string += " " + sess.split("/")[-1] + " : " + ignore_fname + " -- " + " ".join(ignore_lines)

        ignore_strings.append(current_ignore_string)

    elif not (ignore_status == 0 and outputs_status == 1 and spikeInfo_status == 1 and spikeWaveform_status == 1 and sortSummary_status == 1 and splits_chan_status == splits_done_status and (splits_chan_status > 64 or splits_chan_status == 0)):

        incomp_str = sess.split("/")[-1]
        incomp_str += " -- ignore: " + str(ignore_status)
        incomp_str += " -- outputs: " + str(outputs_status)
        incomp_str += " -- spikeInfo: " + str(spikeInfo_status)
        incomp_str += " -- spikeWaveform: " + str(spikeWaveform_status)
        incomp_str += " -- sortSummary: " + str(sortSummary_status)
        incomp_str += " -- split_chan: " + str(splits_chan_status)
        incomp_str += " -- done_chan: " + str(splits_done_status)

        incomplete_strings.append(incomp_str)

        if splits_chan_status == splits_done_status and splits_chan_status > 64:
            incomplete_outputs.append(incomp_str)
            incomplete_outputs_sess.append(sess)
        else:
            incomplete_chans.append(incomp_str)
            incomplete_chans_sess.append(sess.split("/")[-1])
    else:

        complete_strings.append(sess.split("/")[-1])


def split_incomplete_string(s):
    return(s.split(" -- ")[0])


print("--------------------------------------------------")
print("--------------------------------------------------")

for ignore_sess in ignore_strings:

    print(ignore_sess)

print("--------------------------------------------------")

incomplete_outputs.sort(key=split_incomplete_string)
incomplete_chans.sort(key=split_incomplete_string)

print("***** incomplete channel processing")
for incomp_sess in incomplete_chans:

    print(incomp_sess)

print("--------------------------------------------------")
print("***** incomplete output creation")
for incomp_sess in incomplete_outputs:

    print(incomp_sess)

print("--------------------------------------------------")
print("--------------------------------------------------")

print("ignore session: ignore == 1")
print("complete session: ignore == 0, outputs == 1, spikeInfo == 1, spikeWaveform == 1, sortSummary == 1, splits_chan == splits_done")

print()
print()

print("total sessions (with an jacksheet file): " + str(total_count))
print("ignore sessions: " + str(len(ignore_strings)))
print("incomplete sessions: " + str(len(incomplete_strings)))
print("\tincomplete channel processing: " + str(len(incomplete_chans)))
print("\tincomplete output creation: " + str(len(incomplete_outputs)))
print("complete sessions: " + str(len(complete_strings)))

if rerun:

    # create file to rerun spikeInfos

    swarms_path = subj_path + "/_swarms"
    spikeInfo_rerun_big_bash_file = swarms_path + "/sort_rerun_spikeInfo_big_bash.sh"
    spikeInfo_rerun_swarm_file = swarms_path + "/sort_rerun_spikeInfo_swarm.sh"

    rerun_sort_swarm_command = "swarm -g 100 -b 1 -t 2 --time 10:00:00 --gres=lscratch:15 --merge-output --logdir "
    rerun_sort_swarm_command += swarms_path + "/log_dump"
    rerun_sort_swarm_command += " -f " + spikeInfo_rerun_big_bash_file

    spikeInfo_rerun_swarm = open(spikeInfo_rerun_swarm_file, 'w')
    spikeInfo_rerun_swarm.write(rerun_sort_swarm_command)
    spikeInfo_rerun_swarm.close()

    spikeInfo_bash_rerun = open(spikeInfo_rerun_big_bash_file, 'w')

    for sess in incomplete_outputs_sess:
        spikeInfo_bash_rerun.write("bash " + sess + "/spike/spikeInfo.sh" + "\n")

    spikeInfo_bash_rerun.close()

    # create file to rerun entire sessions

    swarms_path = subj_path + "/_swarms"
    rerun_sesslist_fname = swarms_path + "/rerun_sesslist.txt"

    rerun_sesslist = open(rerun_sesslist_fname, 'w')

    for sess in incomplete_chans_sess:
        rerun_sesslist.write(sess + "\n")

    rerun_sesslist.close()

    print("recreating bash scripts for " + str(len(incomplete_chans)) + " sessions ")
    call(["python3", spikes_pipeline_dir + "/construct_bash_scripts.py", subj_path, "--sesslist", rerun_sesslist_fname, "--output_suffix", "rerun"])
