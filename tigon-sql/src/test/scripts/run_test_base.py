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

import getopt
import sys
import re
import os
import os.path
import time


TIGON_ROOT_STR = 'tigon'
TIGON_LOG_ROOT_STR = 'tigon-sql/target/SQL-test-reports'
TIGON_TEST_SCRIPT = 'run_test.pl'

tigon_root_dir = ''

# name of the test
test_name = ''

# log file into test resuts
log_file = None

def run_test(test_dir, subtest_name) :

	print (test_dir)
	print (subtest_name)

	test_status = 'Success'

	script = os.path.join(test_dir, TIGON_TEST_SCRIPT)
	
	log_file.write('Starting: ' + test_name + '/' + subtest_name + '\n')
	
	log_file.write('Running soem stuff\n')
	
	log_file.write('Ending: ' + test_name + '/' + subtest_name + ' ' + test_status + '\n')
	
	return test_status
	
# print usage instructions
def usage():
	print ('Usage: ./run_test.py -t test_name')
	
# check if directory has tigon test in in
def is_test_dir (dir) :
	# a directory must have gsql queries, Input.txt, Expected_Output.txt
	query_found = False
	for fname in os.listdir(dir):
		if fname.endswith ('.gsql') :
			query_found = True
	
	input = os.path.join(dir, 'Input.txt')
	output = os.path.join(dir, 'Expected_Output.txt')
	if os.path.exists(input) and os.path.exists(output) and query_found:
		return True
	return False
		

# find all the test subdirectories
def find_test_dirs (dir) :
	test_dirs = []	
	for fname in os.listdir(dir):
		path = os.path.join(dir, fname)
		if os.path.isdir(path):
			# check if directory has tigon test in in
			if is_test_dir (path) :
				test_dirs.append(path)
			else :
				subdirs =  find_test_dirs(path)
				if subdirs :
					test_dirs.extend(subdirs)
	return test_dirs

def main():

	global test_name, tigon_root_dir, log_file
	
	# list of directories for individual test
	test_dirs = []	

	# process command-line arguments
	try:
		opts, args = getopt.getopt(sys.argv[1:], "ht:v", ["help", "test="])
	except getopt.GetoptError as err:
		# print help information and exit:
		print(str(err))	


	for o, a in opts:
		if o in ("-h", "--help"):
			usage()
			sys.exit(0)
		elif o in ("-t", "--test"):
			test_name = a;
		else:
			print ('Unknown command-line option ' + o)
	
	if test_name == '' :
		usage()
		sys.exit(2)
		
	# find the root directory for Tigon distribution
	# we do it by walkign up the directory tree to find TIGON_ROOT
	cwd = os.getcwd()
	m = re.match('(.+' + TIGON_ROOT_STR + '\/).*', cwd)
	if m:
		tigon_root_dir = m.group(1)
	else :
		print ('Error - test directory is not under ' + TIGON_ROOT_STR)
	
	# open log file
	log_file_name = tigon_root_dir + TIGON_LOG_ROOT_STR + '/' + test_name + '_test_results_' + time.strftime("%Y_%m_%d", time.gmtime()) + '.txt'
	
	log_file = open(log_file_name, "a") 
	if log_file == None :
		print ('Unable to open log file ' + log_file_name)
    	
	# find all the subdirectories with individual tests and add them to the test list
	test_dirs = find_test_dirs(cwd)
	
	print (test_dirs)

	# go through the list of test and run them
	log_file.write('Starting: ' + test_name + '\n')
	
	test_status = 'Success'
	for test_dir in test_dirs :
		status = run_test(test_dir, test_dir[len(cwd)+1:])
		if status == 'Failure' or status == 'SystemError' :
			test_status = status	
	log_file.write('Ending: ' + test_name + ' ' + test_status + '\n')				
		
	# close the log file
	log_file.close()
	
	print (log_file_name)


if __name__ == "__main__":
	main()

