#! /usr/bin/perl
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

