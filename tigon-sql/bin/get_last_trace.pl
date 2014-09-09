#! /usr/bin/perl

$outfile = "last_tracefile.txt";
$infl = "/var/log/gstrace";

if(scalar(@ARGV)>0){
	$infl = $ARGV[0];
}

open I,$infl or die "Can't open input log file $infl\n";

open O,">$outfile";

while($line=<I>){
	if($line =~ /rts:\[.*\]: Started Logging/){
		close O;
		open O,">$outfile";
	}
	print O $line;
}
close O;



