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

@exclude_list = (
	"ext_fcnslexer.cc",
	"ext_fcns.tab.cc",
	"ftalexer.cc",
	"fta.tab.cc",
	"generate_nic_code.cc",
	"ifqlexer.cc",
	"ifq.tab.cc",
	"niclexer.cc",
	"nic.tab.cc",
	"partnlexer.cc",
	"partn.tab.cc",
	"reslexer.cc",
	"res.tab.cc",
	"schemalexer.cc",
	"schema.tab.cc",
	"test_interfacelib.cc",
	"xmllexer.cc",
	"xml.tab.cc"
);

foreach $x (@exclude_list){
	$exclude{$x} = 1;
}

$curr_path = getcwd();
if($curr_path =~ /^(.*\/tigon)\//){
        $prefix = $1;
}else{
        print "didn't find prefix.\n";
        exit(1);
}

$new_path = "$prefix/tigon-sql/src/main/c/ftacmp/";
$ret=chdir($new_path);
if($ret==0){
	print "Can't cd into $new_path.\n";
	exit(2);
}

$glob_str = "$prefix/tigon-sql/src/main/c/ftacmp/*.gcda";
@gcda_files = sort {$a cmp $b} glob($glob_str);

foreach $gfile (@gcda_files){
	$cc_path = $gfile;
	if($cc_path !~ s/[.]gcda/.cc/){
		print "Can't transform file name.\n";
		next;
	}
	if($cc_path =~ /\/([^\/]+)$/){
		$cc_flnm = $1;
		if(defined($exclude{$cc_flnm})){
			next;
		}
	} 

	$open_success = 1;
	$cmd = "gcov $gfile |";
	open I, $cmd or $open_success = 0;
	if($open_success){
		$line=<I>;
		$line=<I>;
		print "$cc_flnm: $line";
	}else{
		print "Can't run gcov on $cc_path\n";
	}
	
}

$ret=chdir($curr_path);
if($ret==0){
	print "Can't cd into $curr_path.\n";
	exit(2);
}
