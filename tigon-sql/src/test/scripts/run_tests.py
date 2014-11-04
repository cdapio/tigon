#!/usr/local/bin/python3.4

# ------------------------------------------------
#   Copyright 2014 AT&T Intellectual Property
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# -------------------------------------------

# Tigon-SQL test harness

import os
import os.path


TIGON_TEST_SCRIPT = 'run_test.sh'



# log file into test resuts
log_file = None

def run_test(test_dir) :
	script_name = os.path.join(test_dir, TIGON_TEST_SCRIPT)
	
	# change into test directory
	os.chdir(test_dir)
	
	print ('Running test ' + test_dir)
	os.system(script_name)

def main():
	# find our workign directory
	cwd = os.getcwd()

	# list of directories for individual test
	test_dirs = []	

	# go through the list of subdirectories and find all the test dirs (the ones with TIGON_TEST_SCRIPT)
	for fname in os.listdir(cwd):
		path = os.path.join(cwd, fname)
		if os.path.isdir(path):
			script_name = os.path.join(path, TIGON_TEST_SCRIPT)
			if os.path.exists(script_name) :
				test_dirs.append (path)
	
	# remove previous test coverage information
	ret = os.system("./clean_coverage_files.pl")
	
	# now execute the tests
	for test_dir in test_dirs :
		run_test (test_dir)
	
	# change the current workign directory back to what it was
	os.chdir(cwd)		
	
	# display test coverage information
	os.system("./extract_coverage_results.pl")	


if __name__ == "__main__":
	main()

