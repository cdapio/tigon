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



