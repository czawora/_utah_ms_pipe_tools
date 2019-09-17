## _utah_ms_pipe_tools

The bottom line:
  * run `python3 construct_bash_scripts.py /path/to/subj/dir`. Then run the sort outputs produced in `subj_dir/_swarms`.
  * use `tally_sessions.py` to asses job completion. add the `--rerun` option to create swarm scripts for selective rerunning of only incomplete jobs.

The inner guts:

These scripts automate the creation of swarm files for subject NSx data uploaded to biowulf via [globus_tools](https://github.com/czawora/globus_tools). This means the session directories in a subject folder should contain:
  * a jacksheetBR_complete.csv,
  * an NEV (optional),
  * an analog pulse file (NS3),
  * a physiology file (NS5/6),
  * and a *_info.txt file (with 3 lines indicating the physiology file extension, the analog pulse file extension, and the the NSP name).

The next step is to make sure your paths in `paths.py` are correct. If you are not part of the biowulf FRNU group, you should be. By the time you are reading this, JW is likely the owner of the group. Ask him to add you. If you are part of the group, then you shouldn't need to change `paths.py`.

When you run `construct_bash_scripts.py` it looks through all the session folders in the path you provide. Within each one that contains the necessary files listed above, it creates a subdirectory, `spike/`, wherein it creates several bash scripts, one for each step of the pipeline. Any logs or outputs from running the pipeline for a session will be in the `spike/` directory. The wrapper script that would run the pipeline for a single session is `spike/sort_run_all.sh` You don't directly need to worry about that wrapper script or the files in each session's subdirectory. The construction script will also gather all the `sort_run_all.sh` files and put them in `subj_path/_swarms/sort_initial_big_bash.sh`. The big bash file is then referenced in the swarm file, `subj_path/_swarms/sort_initial_swarm.sh`, which you ultimately run.

Once all your jobs finish running, use `tally_sessions.py`. It will search each session folder and see how many session folders have all the expected outputs. Passing the `--rerun` option will create new `subj_path/_swarms/sort_rerun_big_bash.sh` and `subj_path/_swarms/sort_rerun_swarm.sh` files that contain only the incomplete sessions that did not reach the final stage of creating the spikeInfo. For sessions that did reach the final stage of creating the spikeInfo but failed, a separate rerun swarm file pair (`subj_path/_swarms/sort_rerun_spikeInfo_big_bash.sh` and `subj_path/_swarms/sort_rerun_spikeInfo_swarm.sh`) are also created. This allows you to only rerun the final step for those sessions. Sessions included in the regular `sort_rerun_swarm.sh` will be rerun completely from scratch. This is helpful for when spikeInfo creation requires more memory than is initially allocated by `construct_bash_scripts.py`. The memory allocated in `subj_path/_swarms/sort_rerun_spikeInfo_swarm.sh` will be an increase from the initial amount, but you can also always edit the rerun spikeInfo swarm file directly if you have diagnosed a specific problem.



(You might notice that the *_info.txt file is redundant. It contains information already present in the jacksheetBR_complete.csv. Right now, it is convenient because it prevents `construct_bash_scripts.py` from having to filter the jacksheet again. Ideally, the same logic in `globus_tools/input/prepare_input_transfer.py` that checked the jacksheetBR_complete.csv, decided what to transfer, and created the info.txt should be separated out into a function that is used in `prepare_input_transfer.py, _utah_lfp_pipe_tools/construct_lfp_bash_scripts.py, and _utah_ms_pipe_tools/construct_bash_scripts.py`. That hasn't happened yet and so the info.txt file is needed. This is also the reason that you need a connection to 56A when you run `prepare_input_transfer.py`. The info.txt files exist there.)
