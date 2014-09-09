#! /usr/bin perl

$infl = "/proc/cpuinfo";
open I,$infl or die "Can't open $infl, exiting.\n";

$curr_proc = -1;
$curr_socket = -1;
$curr_coreid = -1;

while($line=<I>){
	chomp($line);
	@flds = split /:/,$line;
	if(scalar(@flds)==2){
		if($flds[0] =~ /processor/){
			if($curr_proc != -1){
				print "$curr_proc,$curr_socket,$curr_coreid\n";
			}
			$curr_proc = int($flds[1]);
		}
		if($flds[0] =~ /physical id/){
			$curr_socket = int($flds[1]);
		}
		if($flds[0] =~ /core id/){
			$curr_coreid = int($flds[1]);
		}
	}
}
if($curr_proc != -1){
	print "$curr_proc,$curr_socket,$curr_coreid\n";
}
