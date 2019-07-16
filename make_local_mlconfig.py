
import os
import sys
from subprocess import call

if len(sys.argv) < 2:
	print("need to pass in SLURM_JOB_ID")
	exit(1)

jobid = sys.argv[1]

config_path = "/lscratch/" + jobid + "/.config/mountainlab"
config_fpath = config_path + "/mountainlab.user.json"
tmp_path = "/lscratch/" + jobid + "/ms_tmp"

# make the config file findable
call(["export", "HOME=/lscratch/" + jobid])
os.makedirs(config_path, exist_ok=True)

# make the actual tmp path
os.makedirs(tmp_path, exist_ok=True)

# write the lscratch tmp path in the config file
config_fid = open(config_fpath, 'w')

config_fid.write("{\n")
config_fid.write("\"general\": {\n")
config_fid.write("\"temporary_path\": \"" + tmp_path + "\"\n")
config_fid.write("}\n")
config_fid.write("}\n")

config_fid.close()
