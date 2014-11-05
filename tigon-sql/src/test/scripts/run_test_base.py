#!/usr/local/bin/python3.4 -u

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
import subprocess
import difflib


TIGON_ROOT_STR = 'tigon'

# return codes for test results
TEST_SUCCESS = 0
TEST_FAILURE = 1
TEST_SYSERROR = 2

tigon_root_dir = ''

def run_test(test_dir, subtest_name) :
	test_status = 'Success'
	err_code = 0
	
	# change into test directory
	os.chdir(test_dir)
	
	# copy packet schema to cfg directory
	err_code = os.system('cp -p packet_schema_test.txt ' + tigon_root_dir + 'tigon-sql/cfg')
	if err_code != 0 :
		test_status = 'SystemError'
		print('Unable to copy packet_schema_test.txt to ' + tigon_root_dir + 'tigon-sql/cfg')
		return test_status
	
	# create output_spec.cfg file
	output_spec = open('output_spec.cfg', 'w')
	output_spec.write('test_query,stream,,,,,\n');
	output_spec.close()
	
	# remove previously generated output files (they have the form of timestamp.txt)
	for fname in os.listdir(test_dir):
		[name, ext] = os.path.splitext(fname)
		if name.isdigit() :
			os.unlink(fname)
		
	# build a query set
	print('Building a query set')
	buildit_script = tigon_root_dir + 'tigon-sql/bin/buildit_test.pl'
	try:
		output = subprocess.check_output(buildit_script, stderr=subprocess.STDOUT)
		print(str(output, 'utf-8'))		
	except subprocess.CalledProcessError as err:
		test_status = 'SystemError'
		print(str(err.output, 'utf-8'))
		print('Build returned error code ' + str(err.returncode ))	
		return test_status		
	print('Build succeded')	
	
	# execute runit script
	print('Executing runit script')
	runit_pid = subprocess.Popen('./runit', stderr=subprocess.STDOUT, preexec_fn=os.setsid).pid
	
	# sleep a bit to let the runtime complete initialization
	time.sleep(10)
	
	# open gshub.log to find out endpoint for gshub
	if os.path.exists('gshub.log') == False:
		test_status = 'SystemError'	
		print('Error - runit did not generate gshub.log')
		return test_status		
			
	gshub_file = open('gshub.log')
	endpoint = gshub_file.read().rstrip()
	gshub_file.close()
	
	# start gsgdatprint application to subscribe to the output of test_query
	print('Executing ' +  tigon_root_dir + 'tigon-sql/bin/gsgdatprint ' + endpoint + ' default test_query')
	app = subprocess.Popen([tigon_root_dir + 'tigon-sql/bin/gsgdatprint', endpoint, 'default', 'test_query'], stderr=subprocess.STDOUT)
	
	# give an applicatino a chance to start
	time.sleep(5)
	
	# execute start_processing script
	start_script = tigon_root_dir + 'tigon-sql/bin/start_processing'
	print('Executing ' +  start_script)	
	try:
		output = subprocess.check_output(start_script, stderr=subprocess.STDOUT, shell=True)
		print(str(output, 'utf-8'))		
	except subprocess.CalledProcessError as err:
		test_status = 'SystemError'
		print(str(err.output, 'utf-8'))
		print('Error executing start_processing script, error code ' + str(err.returncode ))	
		
	# now wait for gsgdatprint to terminate
	if test_status == 'Success' : 
		# ret = app.poll()
		ret = app.wait()
		if ret is None :
			app.kill()					
		elif ret != 0 :
			test_status = 'SystemError'
			print('Error - error executing ' + tigon_root_dir + 'tigon-sql/bin/gsgdatprint ' + endpoint + ' default test_query')	
			
	# execute stopit script
	print('Executing stopit script')
	try:
		output = subprocess.check_output('./stopit', stderr=subprocess.STDOUT)
		print(str(output, 'utf-8'))			
	except subprocess.CalledProcessError as err:
		test_status = 'SystemError'
		print(str(err.output, 'utf-8'))
		print('stopit returned error code ' + str(err.returncode))	
		return test_status
		
	if test_status != 'Success' : 
		return test_status
	
	# discover output files
	output_file = ''
	for fname in os.listdir(test_dir):
		[name, ext] = os.path.splitext(fname)
		if name.isdigit() :
			output_file = fname
			
	print('Discovered output file ' + output_file)
	
	# convert output file to CSV format
	os.system(tigon_root_dir + 'tigon-sql/bin/gdat2ascii ' + output_file + ' > Output.txt')	
	print('Converted output file to CSV formatted Output.txt')			
	
	# compare obtained and expected output files
	
	print('Comparing query output with expected output')
	file1 = open('Output.txt')
	file2 = open('Expected_Output.txt')
	
	matcher = difflib.SequenceMatcher(None, file1.readlines(), file2.readlines())
	if matcher.real_quick_ratio() < 1.0 :
		print('Query did not produce expected output')
		test_status = 'Failure'		
		
	file1.close()
	file2.close()
	
	# cleanup files
	os.unlink(output_file)	
	os.unlink('Output.txt')
	
	return test_status
	
	
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
	
# print usage instructions
def usage():
	print ('Usage: ./run_test_base.py -t test_name')	

def main():
	global tigon_root_dir
	
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
	    	
	# find all the subdirectories with individual tests and add them to the test list
	test_dirs = find_test_dirs(cwd)

	# go through the list of test and run them
	print('Starting: ' + test_name)
	
	test_status = 'Success'
	for test_dir in test_dirs :
		subtest_name = test_dir[len(cwd)+1:]
		print('Starting: ' + test_name + '/' + subtest_name)		
		status = run_test(test_dir, subtest_name)
		print('Ending: ' + test_name + '/' + subtest_name + ' ' + status)
		
		if status == 'Failure' or status == 'SystemError' :
			test_status = status	
	print('Ending: ' + test_name + ' ' + test_status)				
	
	# change back to working directory we started in
	os.chdir(cwd)
	
	if test_status == 'Success' :
		sys.exit(TEST_SUCCESS)
	elif test_status == 'Failure' :
		sys.exit(TEST_FAILURE)
	else :
		sys.exit(TEST_SYSERROR)


if __name__ == "__main__":
	main()

