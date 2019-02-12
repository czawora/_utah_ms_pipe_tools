

import os
import sys
import glob

if len(sys.argv) < 2:
	print("first argument should be top-level directory containing sorting sessions")
	exit(-1)

subj_dir = sys.argv[1]

if os.path.isdir(subj_dir) == False:
	print("not a real directory: " + subj_dir)
	print("have you logged into the right directory today?")
	exit(-1)

nsx_glob = glob.glob(subj_dir + "/*/*.ns*")

for nsx in nsx_glob:

	sess_path = "/".join(nsx.split("/")[:-1])
	print(sess_path)