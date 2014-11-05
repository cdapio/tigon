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
import time
import sys
import subprocess
import re

TIGON_ROOT_STR = 'tigon'
TIGON_LOG_ROOT_STR = 'tigon-sql/target/SQL-test-reports'

TIGON_TEST_SCRIPT = 'run_test.sh'

tigon_root_dir = ''

def run_test(test_dir, test_name) :
	script_name = os.path.join(test_dir, TIGON_TEST_SCRIPT)
	
	# change into test directory
	os.chdir(test_dir)
	
	# open log file for the test
	log_file_name = tigon_root_dir + TIGON_LOG_ROOT_STR + '/' + test_name + '_test_results_' + time.strftime("%Y_%m_%d", time.localtime()) + '.txt'		
	log_file = open(log_file_name, "a") 
	if log_file == None :
		print ('Unable to open log file ' + log_file_name)
		sys.exit(-1)
	
	# execute test script capturing its stdout and stderr
	try:
		output = subprocess.check_call(script_name, stdout=log_file, stderr=log_file, shell=True)		
	except subprocess.CalledProcessError as err:
		return
		
	# close the log file
	log_file.close()

def main():
	global tigon_root_dir
	
	# find the root directory for Tigon distribution
	# we do it by walkign up the directory tree to find TIGON_ROOT
	cwd = os.getcwd()
	m = re.match('(.+' + TIGON_ROOT_STR + '\/).*', cwd)
	if m:
		tigon_root_dir = m.group(1)
	else :
		print ('Error - test directory is not under ' + TIGON_ROOT_STR)

	# list of directories for individual test
	test_names = []	

	# go through the list of subdirectories and find all the test dirs (the ones with TIGON_TEST_SCRIPT)
	for fname in os.listdir(cwd):
		path = os.path.join(cwd, fname)
		if os.path.isdir(path):
			script_name = os.path.join(path, TIGON_TEST_SCRIPT)
			if os.path.exists(script_name) :
				test_names.append (fname)
	
	# remove previous test coverage information
	ret = os.system("./clean_coverage_files.pl")
	
	# now execute the tests
	for test_name in test_names :
		run_test (os.path.join(cwd, test_name), test_name)
	
	# change the current workign directory back to what it was
	os.chdir(cwd)		
	
	# display test coverage information
	os.system("./extract_coverage_results.pl")	


if __name__ == "__main__":
	main()

