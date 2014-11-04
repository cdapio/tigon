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
import subprocess
import difflib


TIGON_ROOT_STR = 'tigon'
TIGON_LOG_ROOT_STR = 'tigon-sql/target/SQL-test-reports'
TIGON_TEST_SCRIPT = 'run_test.pl'

tigon_root_dir = ''

# name of the test
test_name = ''

# log file into test resuts
log_file = None

def run_test(test_dir, subtest_name) :
	test_status = 'Success'
	err_code = 0
	
	# change into test directory
	os.chdir(test_dir)
	
	# copy packet schema to cfg directory
	err_code = os.system('cp -p packet_schema_test.txt ' + tigon_root_dir + 'tigon-sql/cfg')
	if err_code != 0 :
		test_status = 'SystemError'
		log_file.write('Unable to copy packet_schema_test.txt to ' + tigon_root_dir + 'tigon-sql/cfg\n')
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
	log_file.write('Building a query set\n')
	buildit_script = tigon_root_dir + 'tigon-sql/bin/buildit_test.pl'
	try:
		output = subprocess.check_output(buildit_script, stderr=subprocess.STDOUT)
		log_file.write(str(output, 'utf-8'))		
	except subprocess.CalledProcessError as err:
		test_status = 'SystemError'
		log_file.write(str(err.output, 'utf-8'))
		log_file.write('Build returned error code ' + str(err.returncode ) + '\n')	
		return test_status		
	log_file.write('Build succeded\n')	
	
	# execute runit script
	log_file.write('Executing runit script\n')
	log_file.flush()	
	runit_pid = subprocess.Popen('./runit', stdout=log_file, stderr=log_file, preexec_fn=os.setsid).pid
	
	# sleep a bit to let the runtime complete initialization
	time.sleep(10)
	
	# open gshub.log to find out endpoint for gshub
	if os.path.exists('gshub.log') == False:
		test_status = 'SystemError'	
		log_file.write('Error - runit did not generate gshub.log\n')
		return test_status		
			
	gshub_file = open('gshub.log')
	endpoint = gshub_file.read().rstrip()
	gshub_file.close()
	
	# start gsgdatprint application to subscribe to the output of test_query
	log_file.write('Executing ' +  tigon_root_dir + 'tigon-sql/bin/gsgdatprint ' + endpoint + ' default test_query\n')
	app = subprocess.Popen([tigon_root_dir + 'tigon-sql/bin/gsgdatprint', endpoint, 'default', 'test_query'], stdout=log_file, stderr=log_file)
	
	# give an applicatino a chance to start
	time.sleep(5)
	
	# execute start_processing script
	start_script = tigon_root_dir + 'tigon-sql/bin/start_processing'
	log_file.write('Executing ' +  start_script + '\n')	
	try:
		output = subprocess.check_output(start_script, stderr=subprocess.STDOUT, shell=True)
		log_file.write(str(output, 'utf-8'))		
	except subprocess.CalledProcessError as err:
		test_status = 'SystemError'
		log_file.write(str(err.output, 'utf-8'))
		log_file.write('Error executing start_processing script, error code ' + str(err.returncode ) + '\n')	
		
	# now wait for gsgdatprint to terminate
	if test_status == 'Success' : 
		# ret = app.poll()
		ret = app.wait()
		if ret is None :
			app.kill()					
		elif ret != 0 :
			test_status = 'SystemError'
			log_file.flush()		
			log_file.write('Error - error executing ' + tigon_root_dir + 'tigon-sql/bin/gsgdatprint ' + endpoint + ' default test_query\n')	
			
	# execute stopit script
	log_file.write('Executing stopit script\n')
	try:
		output = subprocess.check_output('./stopit', stderr=subprocess.STDOUT)
		log_file.write(str(output, 'utf-8'))			
	except subprocess.CalledProcessError as err:
		test_status = 'SystemError'
		log_file.write(str(err.output, 'utf-8'))
		log_file.write('stopit returned error code ' + str(err.returncode ) + '\n')	
		return test_status
		
	if test_status != 'Success' : 
		return test_status
	
	# discover output files
	output_file = ''
	for fname in os.listdir(test_dir):
		[name, ext] = os.path.splitext(fname)
		if name.isdigit() :
			output_file = fname
			
	log_file.write('Discovered output file ' + output_file + '\n')
	
	# convert output file to CSV format
	os.system(tigon_root_dir + 'tigon-sql/bin/gdat2ascii ' + output_file + ' > Output.txt')	
	log_file.write('Converted output file to CSV formatted Output.txt\n')			
	
	# compare obtained and expected output files
	
	log_file.write('Comparing query output with expected output\n')
	file1 = open('Output.txt')
	file2 = open('Expected_Output.txt')
	
	matcher = difflib.SequenceMatcher(None, file1.readlines(), file2.readlines())
	if matcher.real_quick_ratio() < 1.0 :
		log_file.write('Query did not produce expected output\n')
		test_status = 'Failure'		
		
	file1.close()
	file2.close()
	
	# cleanup files
	os.unlink(output_file)	
	os.unlink('Output.txt')
	
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
	log_file_name = tigon_root_dir + TIGON_LOG_ROOT_STR + '/' + test_name + '_test_results_' + time.strftime("%Y_%m_%d", time.localtime()) + '.txt'
	
	log_file = open(log_file_name, "a") 
	if log_file == None :
		print ('Unable to open log file ' + log_file_name)
    	
	# find all the subdirectories with individual tests and add them to the test list
	test_dirs = find_test_dirs(cwd)

	# go through the list of test and run them
	log_file.write('Starting: ' + test_name + '\n')
	
	test_status = 'Success'
	for test_dir in test_dirs :
		subtest_name = test_dir[len(cwd)+1:]
		log_file.write('Starting: ' + test_name + '/' + subtest_name + '\n')		
		status = run_test(test_dir, subtest_name)
		log_file.write('Ending: ' + test_name + '/' + subtest_name + ' ' + status + '\n')
		
		if status == 'Failure' or status == 'SystemError' :
			test_status = status	
	log_file.write('Ending: ' + test_name + ' ' + test_status + '\n')				
		
	# close the log file
	log_file.close()
	
	# change back to working directory we started in
	os.chdir(cwd)
	
	if test_status == 'Success' :
		sys.exit(0)
	elif test_status == 'Failure' :
		sys.exit(1)
	else :
		sys.exit(2)


if __name__ == "__main__":
	main()

