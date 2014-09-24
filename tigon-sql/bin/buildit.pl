#! /usr/bin/perl

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

use Cwd;

$rootdir_name = "tigon";
$curr_path = getcwd();


if($curr_path =~ /^(.*\/tigon)\//){
	$prefix = "$1/tigon-sql";
}else{
	print "didn't find prefix.\n";
	exit(1);
}

$cmd1 =  "$prefix/bin/translate_fta -h localhost -c -M -C $prefix/cfg -l $prefix/qlib -R $prefix packet_schema.txt *.gsql";

print "Executing $cmd1\n";
$ret = system($cmd1);
if($ret){
	print "ERROR build failed.\n";
	return $ret;
}

$ret = system("make");
if($ret){
	print "ERROR build failed.\n";
}
$ret;

