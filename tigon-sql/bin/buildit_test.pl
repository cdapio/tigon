#! /usr/bin/perl
use Cwd;

$curr_path = getcwd();


if($curr_path =~ /^(.*\/tigon)\//){
	print $1
	$prefix = "$1/tigon-sql";
}else{
	print "didn't find prefix.\n";
	exit(1);
}

$cmd1 =  "$prefix/bin/translate_fta.gcov -h localhost -c -M -C $prefix/cfg -l $prefix/qlib -R $prefix packet_schema_test.txt *.gsql";

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

