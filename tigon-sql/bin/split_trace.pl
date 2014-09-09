#! /usr/bin/perl

$outfile_base = "tracefile_";
$trace_cnt = 0;

$outfile = $outfile_base . $trace_cnt . ".txt";
open O,">$outfile";

while($line=<>){
	if($line =~ /rts:\[.*\]: Started Logging/){
		close O;
		$trace_cnt ++;
		$outfile = $outfile_base . $trace_cnt . ".txt";
		open O,">$outfile";
	}
	print O $line;
}
close O;



