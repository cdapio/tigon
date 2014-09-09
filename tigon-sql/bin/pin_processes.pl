#! /usr/bin/perl

$infl = "/var/log/gstrace";

if(scalar(@ARGV)>0){
	$infl = $ARGV[0];
}

open I,$infl or die "Can't open input log file $infl\n";

%rts_pids= ();
%hfta_pids = ();
while($line=<I>){
	if($line =~ /rts:\[.*\]: Started Logging/){
		%rts_pids= ();
		%hfta_pids = ();
	}
	if($line =~ /hfta_(\d+):\[(\d+)\]: Started Logging/){
		$name = "hfta_" . $1;
		$hfta_pids{$name} = $2;
	}
	if($line =~ /rts:\[(\d+)\]: Init LFTAs for (\w+)/){
		$name = "rts " . $2;
		$rts_pids{$name} = $1;
	}
}

#foreach $k (keys(%rts_pids)){
#	print "$k, ",$rts_pids{$k},"\n";
#}
#foreach $k (keys(%hfta_pids)){
#	print "$k, ",$hfta_pids{$k},"\n";
#}
		
open C,"pinning_info.csv" or die "Can't open pinning_info.csv\n";

while($line=<C>){
	chomp($line);
	@flds = split /,/,$line;
	if(exists($rts_pids{$flds[0]})){
		$pinning_cmd = "taskset -pc $flds[1] ". $rts_pids{$flds[0]};
	}
	if(exists($hfta_pids{$flds[0]})){
		$pinning_cmd = "taskset -pc $flds[1] ". $hfta_pids{$flds[0]};
	}
	print "Executing $pinning_cmd\n";
	system($pinning_cmd);
}

