#!/usr/bin/perl
use Digest::MD5 qw(md5 md5_hex md5_base64);
$GSROOT=`cd ../../; pwd`;
chomp $GSROOT;
$SCHEMA=$ARGV[0];
$QUERY=$ARGV[1];
$INPUTFILENAME=$ARGV[2];
@S=split /;/, $SCHEMA;
@Q=split /;/,$QUERY;

$h=md5_base64("$SCHEMA;$QUERY;$INPUTFILENAME");
$host=`hostname`;
chomp $host;

$id="work/$h";

#parse query from the command line and create query file

system("mkdir -p $id/built");
open(O,">$id/built/stream.gsql");
print O "DEFINE {\nquery_name 'output';\n}\n";
for($x=0;$x<scalar(@Q);$x++) {
print O "$Q[$x]\n";
}
close(O);

#creaete host definitions for a single CSV interface for singlefile operation 

open(O,">$id/built/$host.ifq");
print O "default: Equals[Host,'$host']";
close(O);

open(O,">$id/built/ifres.xml");
print O "<Resources>\n";
print O "<Host Name='$host'>\n";
print O "<Interface Name='CSV0'>\n";
print O "<InterfaceType value='CSV'/>\n";
print O "<Filename value='$INPUTFILENAME'/>\n";
print O "<StartupDelay value='10'/>\n";
print O "<CSVSeparator value='|'/>\n";
print O "<SingleFile value='TRUE'/>\n";
#print O "<Verbose value='TRUE'/>\n";
print O "</Interface>\n";
print O "</Host>\n";
print O "</Resources>\n";
close (O);

#create schema for feed based on simplieded schema definition from the command line

open(O,">$id/built/packet_schema.txt");
print O "PROTOCOL base{\n";
print O "uint systemTime get_system_time (low_cardinality,required,increasing, snap_len 0);\n";
print O "}\n";
print O "PROTOCOL $S[0] (base) {\n";
for($x=1;$x<scalar(@S);$x++) {
@V=split / /, $S[$x];
$type=lc $V[0];
print O "$S[$x] get_csv_$type\_pos$x;\n";
}
print O "}\n";

#create output specifications

open(O,">$id/built/output_spec.cfg");
print O "output,stream,,,,,\n";
close(O);

#create a runall script

open(O,">$id/runall");
print O "#!/bin/sh\n";
print O "killall -9 gsprintconsole\n";
print O "./runit\n";
print O "./gsprintconsole output\n";
close(O);

system("chmod 777 $id/runall");

#genereate the C and C++ code as well as the make file

system("cd $id/built; $GSROOT/bin/translate_fta -M -C . packet_schema.txt stream.gsql");

#fix the makefile as this is not being built in the default demo directory this should really be an option for translate_fta

system("cd $id/built; sed -e \"s|../../include|$GSROOT/include|g\"  -e \"s|../../lib|$GSROOT/lib|g\" -e\"s|../../cfg|$GSROOT/cfg|g\" -e \"s|../../bin|$GSROOT/bin|g\" Makefile > Makefile.relocated");

#fix linux issues in makefile this is a bug in translate_fta and should get fixed

system("cd $id/built; sed -e \"s/gcc -O -g -o rts/g++ -O -g -o rts/g\" -e \"s/-O/-O4/g\" -e \"s/-lz//g\" -e \"s/-lfl//g\" -e \"s/-lclearinghouse/-lclearinghouse -lresolv -lpthread -lgscpinterface /g\"  -e \"s/-lpcap//g\" Makefile.relocated > Makefile.relocated.linux");

#now built everything

system("cd $id/built; make -f Makefile.relocated.linux");

# now copy all the binaries to the root

system("cd $id/; cp built/stopit .; cp built/set_vinterface_hash.bat . ; cp built/runit . ; cp built/rts . ; cp built/hfta_[^.] .");

#links toold I need

system("ln -s $GSROOT/bin/gsprintconsole $id/");

# link the data file in the project directory

system(" ln -s `pwd`/$INPUTFILENAME $id/");

# finally run the whole thing

system ("cd $id; ./runall");

# if it's done clean everything up

system ("cd $id; ./stopit");
