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

use English;

$interval = 10;
$to_tty = 0;
if(scalar(@ARGV)>0){
	if(int($ARGV[0])>0){
		$interval = int($ARGV[0]);
	}else{
		if($ARGV[0] eq '-'){
			$to_tty = 1;
		}else{
			print STDERR "Argument 1 ($ARGV[0]) ignored.\n";
		}	
	}
}
if(scalar(@ARGV)>1){
	if(int($ARGV[1])>0){
		$interval = int($ARGV[1]);
	}else{
		if($ARGV[1] eq '-'){
			$to_tty = 1;
		}else{
			print STDERR "Argument 2 ($ARGV[1]) ignored.\n";
		}	
	}
}
if(scalar(@ARGV)>2){
	print STDERR "Only 2 arguments accepted, additional args ignored.\n";
}

open O, ">resource_log.csv" or die "Can't open resource_log.csv\n";



# get the page size
open I,"getconf PAGESIZE |";
$line=<I>;
chomp($line);
$pagesize = int($line);
close(I);


$effective_uid = $REAL_USER_ID;
$ps_cmd = "ps -U $effective_uid |";

%hfta_pid=();

open P,$ps_cmd;
while($line=<P>){
	chomp($line);
	$line =~ s/^\s+//;
	@flds = split /\s+/,$line;
	if($flds[3] =~ /rts/){
		push @rts_pids,int($flds[0]);
	}
	if($flds[3] =~ /hfta_(\d+)/){
		$hfta_pid{$1} = int($flds[0]);
	}
}

$rts_pids = sort {$a<=>$b} @rts_pids;

$now=time();
print O "START monitoring at time $now\n";
print O "ts,proc,pid,utime,stime,vm_size,rss,pagesize\n";
if(to_tty){
	print "START monitoring at time $now\n";
	print "ts,proc,pid,utime,stime,vm_size,rss,pagesize\n";
}
#print "RTS PIDs:\n";
#foreach $p (@rts_pids){
#	print "$p ";
#}
#print "\nHFTA PIDs:\n";
#  foreach $h (sort {$a<=>$b} keys(%hfta_pid)){
#	print "($h,",$hfta_pid{$h},") ";
#}
#print "\n";

while(1){
	$now=time();
	foreach $p (@rts_pids){
		get_proc_stats($p);
		print O "$now,rts,$p,$user_time,$sys_time,$vm_size,$resident_set_size,$pagesize\n";
		if(to_tty){
			print "$now,rts,$p,$user_time,$sys_time,$vm_size,$resident_set_size,$pagesize\n";
		}
	}
	foreach $h (sort {$a<=>$b} keys(%hfta_pid)){
		$p=$hfta_pid{$h};
		get_proc_stats($p);
		print O "$now,hfta_$h,$p,$user_time,$sys_time,$vm_size,$resident_set_size,$pagesize\n";
		if(to_tty){
			print "$now,hfta_$h,$p,$user_time,$sys_time,$vm_size,$resident_set_size,$pagesize\n";
		}
	}

	sleep($interval);
}



sub get_proc_stats{
my $pid = shift;
my $line;
my @flds;
my $pos;


open I_PROC_STATS,"/proc/$pid/stat";

$line=<I_PROC_STATS>;
close I_PROC_STATS;

#print "getstats($pid): $line\n";

chomp($line);
@flds = split / +/,$line;

$pos=0;
$pid = $flds[$pos]; $pos++;
$fname = $flds[$pos]; $pos++;
$state = $flds[$pos]; $pos++;
$pppid = $flds[$pos]; $pos++;
$pgrp = $flds[$pos]; $pos++;
$session = $flds[$pos]; $pos++;
$tty_n = $flds[$pos]; $pos++;
$tpgid = $flds[$pos]; $pos++;
$flags = $flds[$pos]; $pos++;
$minor_faults = $flds[$pos]; $pos++;
$child_minor_faults = $flds[$pos]; $pos++;
$major_faults = $flds[$pos0]; $pos++;
$child_major_faults = $flds[$pos0]; $pos++;
$user_time = $flds[$pos]; $pos++;
$sys_time = $flds[$pos]; $pos++;
$child_user_time = $flds[$pos]; $pos++;
$child_sys_time = $flds[$pos]; $pos++;
$priority = $flds[$pos]; $pos++;
$nice = $flds[$pos]; $pos++;
$num_threads = $flds[$pos]; $pos++;
$interval_timer_realvalue = $flds[$pos]; $pos++;
$start_time = $flds[$pos]; $pos++;
$vm_size = $flds[$pos]; $pos++;
$resident_set_size = $flds[$pos]; $pos++;
$rss_limit = $flds[$pos]; $pos++;
$startcode = $flds[$pos]; $pos++;
$endcode = $flds[$pos]; $pos++;
$startstack = $flds[$pos]; $pos++;
$curr_kernel_stack_ptr = $flds[$pos]; $pos++;
$curr_instruction_ptr = $flds[$pos]; $pos++;
$signal = $flds[$pos]; $pos++;
$sigblocked = $flds[$pos]; $pos++;
$sigignore = $flds[$pos]; $pos++;
$wchan = $flds[$pos]; $pos++;
$nswap = $flds[$pos]; $pos++;
$exit_signal = $flds[$pos]; $pos++;
$processor = $flds[$pos]; $pos++;
$rt_priority = $flds[$pos]; $pos++;
$policy = $flds[$pos]; $pos++;
$aggr_block_io_delays = $flds[$pos]; $pos++;
$guest_time = $flds[$pos]; $pos++;
$cguest_time = $flds[$pos]; $pos++;
}
