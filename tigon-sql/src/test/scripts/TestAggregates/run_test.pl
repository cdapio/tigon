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
use Sys::Hostname;
use POSIX('uname');

sub GetProcess;
sub KillAll;
sub Ps;
sub ExitWithError;
sub Die;
sub LogMessage;
sub InspectOutput($$$$$$\$);

$| = 1;

use constant { SUCCESS => 0, FAILURE => 1, SYSERROR => 2, TIMESTAMP => 3, NO_TIMESTAMP => 4 };
%ErrorStrings = ( SUCCESS => "Success", FAILURE => "Failure", SYSERROR => "SystemError",
                  0 => "Success", 1 => "Failure", 2 => "SystemError" );
$ID=`id -un`;
chomp $ID;

$TestName = "TestAggregates";

$PWD = cwd();
($STREAMING) = $PWD =~ /^(.*\/STREAMING\b)/;
$STREAMING="$ENV{HOME}/STREAMING" if ( ! defined $STREAMING );
Die "Could not identify STREAMING directory." if ( ! -d $STREAMING );
$STREAMING_TEST="$STREAMING/test";
$ROOT="$STREAMING_TEST/$TestName";
%Months = ( 1 => "Jan",
            2 => "Feb",
            3 => "Mar",
            4 => "Apr",
            5 => "May",
            6 => "Jun",
            7 => "Jul",
            8 => "Aug",
            9 => "Sep",
            10 => "Oct",
            11 => "Nov",
            11 => "Dec" );
$HostName = hostname();
($Uname) = uname();
$GID = getpgrp($$);
if ( $Uname eq 'Darwin' ) { $GID = "g${GID}"; }
@FilesToDelete = ("${HostName}_lfta.c", "external_fcns.def", "gswatch.pl", "hfta_0", "hfta_0.cc", "hfta_0.o", "internal_fcn.def", "lfta.o", "Makefile", "Output.txt", "postopt_hfta_info.txt", "preopt_hfta_inf", "qtree.xml", "rts", "runit", "set_vinterface_hash.bat", "stopit" );
@ProcessList = ('gshub.py', 'rts', 'hfta_0', 'hfta_1', 'hfta_2', 'gsprintconsole', 'gen_feed', 'gsgdatprint', 'gendata.pl');

$ExitCode = SUCCESS;
my $LogFile;
{
    my ($Second,$Minute,$Hour,$DayOfMonth,$Month,$Year) = localtime();
    $Year += 1900;
    $Month += 1;
    $LogFile= sprintf("$STREAMING/test/test_results_%04d-%02d-%02d.txt",
                      $Year , $Month, $DayOfMonth);
}

LogMessage( "Starting: <$TestName>", NO_TIMESTAMP );

Die "File $STREAMING_TEST/$TestName/packet_schema_test.txt is expected." unless ( -f "$STREAMING_TEST/$TestName/packet_schema_test.txt" );
if ( system("cp", "-p", "$STREAMING_TEST/$TestName/packet_schema_test.txt", "$STREAMING/cfg/packet_schema_test.txt") )
{
    Die "Could not copy packet_schema_test.txt into $STREAMING/cfg";
}

chdir "$ROOT" or Die "Could not change directory to $ROOT: $!";
opendir DIR, "." or Die "Could not open current directory for read.";
@DirectoryList = readdir DIR;
closedir DIR;
@Aggregates=();

foreach my $Item ( @DirectoryList )
{
    if ( $Item eq "." or $Item eq ".." ) { next; }
    if ( -d $Item and ! -l $Item )
    {
        push @Aggregates, $Item;
    }
}


if ( @ARGV == 1 and -d $ARGV[0] )
{
    @Aggregates=@ARGV;
}
elsif ( @ARGV > 1 or (@ARGV == 1 and ! -d $ARGV[0]) )
{
    Die "Invalid arguments @ARGV.";
}

open FILE, "$STREAMING/bin/gshub.py" or Die "Could not open file $STREAMING/bin/gshub.py for read.";
$Python = <FILE>;
close FILE;
($Python) = $Python =~ /^\#\!(.+)$/g;

$Ag_Count = 0;
foreach my $Ag_Type (@Aggregates)
{
    if ( $Ag_Count++ > 0 )
    { LogMessage "===============================================================", NO_TIMESTAMP; }
    chdir "$ROOT/$Ag_Type" or Die "Could not change directory to $ROOT/$Ag_Type: $!";
####    LogMessage "Working on type $Ag_Type in $ROOT/$Ag_Type.";
    LogMessage "Starting: <$TestName/$Ag_Type>", NO_TIMESTAMP;
    
    opendir DIR, "." or Die "Could not open include directory for read.";
    my @DirectoryList = readdir DIR;
    closedir DIR;
    my @QueryNames;
    open OUTSPEC, ">output_spec.cfg" or Die "Could not open file output_spec.cfg for write.";
    foreach my $Item ( @DirectoryList )
    {
    
        if ( $Item eq ".." or $Item eq "." ) {next;}
        if ( $Item =~ /^[a-zA-Z_0-9]+\.gsql/ )
        {
###            push @QueryFiles, $Item;
            open FILE, "$Item" or Die "Could not open query file $Item.";
            my $QueryName;
            while (<FILE>)
            {   
                if ( $_ =~ /^query_name / ) 
                {   
                    ($QueryName) = $_ =~ /^query_name +'([a-zA-Z_0-9]+)'/;
                    if ( defined $QueryName) { last; }
                }   
            }   
            close FILE;
            if ( ! defined $QueryName )
            {
                LogMessage "Could not extract query name from $Item in $ROOT/$Ag_Type.", SYSERROR;
                next;
            }
            print OUTSPEC "${QueryName},stream,,,,,\n";
            push @QueryNames, $QueryName;
        }
    }

    close OUTSPEC;

    Die "Could not locate any query files in $ROOT/$Ag_Type." if ( ! @QueryNames );
    open FILE, ">gen_feed" or Die "Could not open file gen_feed for write.";
    print FILE q(#!/usr/bin/perl
while(1==1) {
while((-e "exampleCsv")) { sleep 1; }
system("cp Input.txt exampleCsvX ; mv exampleCsvX exampleCsv");
sleep 5;
}
);
    close FILE;
    Die "Could not chmod on $ROOT/$Ag_Type/gen_feed." unless chmod(0777, "gen_feed") == 1;

    opendir DIR, "." or Die "Could not open current directory for read.";
    @DirectoryList = readdir DIR;
    closedir DIR;
    foreach my $FileToDelete (@FilesToDelete)
    {
        foreach my $File ( @DirectoryList )
        {
            if ( $File eq $FileToDelete )
            {
                Die "Could not remove file $File" unless unlink($File) == 1;
                last;
            }
        }
    }
    my $Command = "$STREAMING/bin/buildit_test.pl > buildit.out 2>&1";
    print "$Command\n";
    if (system("$Command") or ! -f "rts" or ! -f "lfta.o" )
    {
        LogMessage "buildit for $Ag_Type failed.", SYSERROR;
        LogMessage "Ending: <$TestName/$Ag_Type> <$ErrorStrings{SYSERROR}>", NO_TIMESTAMP;
        next;
    }
    LogMessage "buildit in $ROOT/$Ag_Type successful.";
    foreach my $QueryName ( @QueryNames )
    {
        LogMessage "Starting: <$TestName/$Ag_Type/$QueryName>", NO_TIMESTAMP;
        opendir DIR, "." or Die "Could not open current directory for read.";
        @DirectoryList = readdir DIR;
        closedir DIR;
        foreach my $File ( @DirectoryList )
        {
            if ( ! -f $File or $File !~ /^[0-9]+.txt/ ) {next;}
            Die "Could not remove file $File." unless unlink($File) == 1;
        }

        KillAll;
        Die "Could not remove file gs.pids" if ( -f "gs.pids"  && unlink("gs.pids") != 1 );

#        my $RC = system("./runit > run.out 2>&1");

	my $pid = fork();
	if (not defined $pid) {
   		die 'Unable to fork child process';
	} elsif ($pid == 0) {
    # CHILD
		setpgrp;
    	exit system("./runit > run.out 2>&1");
	} else {
   		wait;
    	$RC = $?;
	}

        sleep 1;
        if ( $RC != 0  )
        {
            my $ErrorMessage = "runit failed to run rts.\n";
            open FILE, "run.out" or Die "Could not open file run.out.";
            my $LineCount = 0;
            while (<FILE>)
            {
                if ( $LineCount++ == 0 ) { $ErrorMessage = $ErrorMessage . "Output is shown below:\n"; }
                $ErrorMessage = $ErrorMessage . $_;
            }
            close FILE;
            LogMessage $ErrorMessage, SYSERROR;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{SYSERROR}>", NO_TIMESTAMP;
            next;
        }

        sleep 10;
        if ( ! open GSHUB, "gshub.log" )
        {
            my $Message = "Could not open file gshub.log";
            if ( ! -f $Python or ! -x $Python )
            {
                $Message = $Message . " as $Python does not exist.";
            }
            Die "$Message\nPlease make sure you have python 3.x installed and firewall is configured to allow opening port to listen to HTTP requests.";
        }

        my $IP = <GSHUB>;
        close GSHUB;
        chomp($IP);
        if ( length($IP) < 10 )
        {
            LogMessage "Can not find hub ip in $ROOT/$Ag_Type.", SYSERROR;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{SYSERROR}>", NO_TIMESTAMP;
            next;
        }

        print "${STREAMING}/bin/gsgdatprint $IP default $QueryName\n";
        system("${STREAMING}/bin/gsgdatprint $IP default $QueryName > gsgdatprint.out 2>&1&");
        sleep 1;
        if (Ps('gsgdatprint') == 0)
        {
            my $ErrorMessage = "Could not run gsgdatprint. Errors printed by gsgdatprint were:\n";
            open FILE, "gsgdatprint.out" or Die "Could not open file gsgdatprint.out.";
            while (<FILE>) { $ErrorMessage = $ErrorMessage . $_; }
            close FILE;
            LogMessage $ErrorMessage, SYSERROR;
            next;
        }

        sleep 10;
        system("./gen_feed 2>&1&");
        sleep 1;
        Die "Could not run gen_feed." unless Ps("gen_feed") == 1;
        Ps;

        if ( ! -f "Expected_${QueryName}.txt" )
        {
            LogMessage "Could not locate file Expected_${QueryName}.txt in $ROOT/$Ag_Type.", SYSERROR;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{SYSERROR}>", NO_TIMESTAMP;
            next;
        }
        open FILE, "Expected_${QueryName}.txt" or Die "Could not open Expected_${QueryName}.txt for read.";
        my $ExpectedLineCount = 0;
        my ($ExpectedContent, @ExpectedContent);
        while (<FILE>)
        {
            ++$ExpectedLineCount;
            s/^[0-9]*\|//;
            push @ExpectedContent, $_;
        }
        close FILE;
        $ExpectedContent = join "", sort @ExpectedContent;
        if ( $ExpectedLineCount == 0 )
        {
            LogMessage "Could not retrieve expected lines from file Expected_${QueryName}.txt in $ROOT/$Ag_Type.", SYSERROR;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{SYSERROR}>", NO_TIMESTAMP;
            next;
        }

        my $DiffChecked = 0;
        for ( my $Count = 59; $Count >= 0 and $DiffChecked == 0; --$Count )
        {
            sleep 2;
            InspectOutput($TestName, $Ag_Type, $QueryName, $Count, $ExpectedLineCount, $ExpectedContent, $DiffChecked);
        }

        KillAll;

        if ( $DiffChecked == 0 )
        {
            LogMessage "Can not identify valid text file expected from gsgdatprint for query $Ag_Type/$QueryName.", FAILURE;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{FAILURE}>", NO_TIMESTAMP;
        }
    }
}


if ( $ExitCode == SUCCESS )
{
    LogMessage "All $TestName comparisons succeeded.";
}
LogMessage "Ending: <$TestName> <$ErrorStrings{$ExitCode}>\n", NO_TIMESTAMP;
LogMessage("----------------------------------------------------------------------------------------------", NO_TIMESTAMP);
exit $ExitCode;


sub GetProcess
{
    my @PROCESSES = ();
    if  ( @_ > 1 or !defined $_[0] or $_[0] eq "" )
    {
        print STDERR "Invalid parameters @_\n";
        return undef;
    }
    my $JobToKill = $_[0];
    open PS, "ps -$GID|" or Die "Could not open ps command: $!";
    while ( my $PROCESS=<PS> )
    {
        if ( $PROCESS =~ /\b${JobToKill}\b/ )
        {
            $PROCESS =~ s/^ *//;
            my @ProcessDetails = split /  */, $PROCESS;
            if ( $ProcessDetails[0] != $$ ) { push @PROCESSES, $ProcessDetails[0]; }
        }
    }
    close PS;
    return @PROCESSES;
}

sub KillAll
{
	$ret = system("./stopit");
	if($ret != 0){
		print "Warning, could not execute ./stopit, return code is $ret\n";
	}

    my @PROCESSES = ();
    foreach my $PROCESS (@ProcessList)
    {
        push @PROCESSES, GetProcess $PROCESS
    }
    if ( @PROCESSES ){print "kill -KILL @PROCESSES\n";}
    if ( @PROCESSES and kill( 'KILL', @PROCESSES ) != @PROCESSES )
    {
        LogMessage "Could not kill processes @PROCESSES.", SYSERROR;
        return SYSERROR;
    }
    return 0;
}

sub Ps
{
    my $QueriedProcess;
    if  ( @_ > 1 or (@_ == 1 && (!defined $_[0] or $_[0] eq "")) )
    {
        print STDERR "Invalid parameters @_\n";
        exit SYSERROR;
    }
    elsif ( @_ == 1 )
    {
        $QueriedProcess = shift;
        if ( !exists {map{ $_ => 1 } @ProcessList}->{$QueriedProcess} )
        {
            print STDERR "The parameter $QueriedProcess is not one of the expected process names: @ProcessList.";
            exit SYSERROR;
        }
    }

    open PS, "ps -$GID|" or Die "Could not open ps command: $!";
    my $Count = 0;
    while ( my $PROCESS=<PS> )
    {
        foreach my $TargetProcess (@ProcessList)
        {
            if ( $PROCESS =~ /\b${TargetProcess}\b/ )
            {
                if ( defined $QueriedProcess and $QueriedProcess eq $TargetProcess )
                {
                    ++$Count;
                    last;
                }
                elsif ( defined $QueriedProcess )
                {
                    next;
                }
                ++$Count;
                print $PROCESS;
                last;
            }
        }
    }
    close PS;
    return $Count;
}


sub ExitWithError
{
    if  ( @_ > 2 or !defined $_[0] or $_[0] eq "" )
    {
        print STDERR "Invalid parameters @_\n";
        exit SYSERROR;
    }

    my $ExitCode = FAILURE;
    if ( @_ == 2 and $_[1] != FAILURE and $_[1] != SYSERROR )
    {
        print STDERR "Invalid parameters @_\n";
        exit SYSERROR;
    }
    elsif ( @_ == 2 )
    {
        $ExitCode = $_[1];
    }

    LogMessage $_[0], $ExitCode;
    KillAll;
    exit $ExitCode;
}

sub Die
{
    if  ( @_ > 1 or !defined $_[0] or $_[0] eq "" )
    {
        print STDERR "Invalid parameters @_\n";
        exit SYSERROR;
    }
    my $PWD = cwd();
    my ($PARENT_DIR, $CURRENT_DIR) = $PWD =~ /^(.*)\/([^\/]+)$/;
    ($PARENT_DIR) = $PARENT_DIR =~ /^.*\/([^\/]+)$/;
    LogMessage $_[0], SYSERROR;
    KillAll;
    if ( $PARENT_DIR eq "$TestName" or $CURRENT_DIR eq "$TestName" )
    {
        my $DIR_DETAILS = "$TestName";
        if ( $CURRENT_DIR ne "$TestName" ) { $DIR_DETAILS = "$DIR_DETAILS/$CURRENT_DIR"; }
        LogMessage "Ending: <$DIR_DETAILS> <$ErrorStrings{SYSERROR}>", NO_TIMESTAMP;
    }
    exit SYSERROR;
}

sub LogMessage
{
    my $TimeStampFlag = TIMESTAMP;
    if  ( @_ > 3 or !defined $_[0] or $_[0] eq "" )
    {
        print STDERR "Invalid parameters passed to LogMessage()\n";
        exit SYSERROR;
    }
    my $Message = shift;
    chomp $Message;
    my $CurrentErrorCode = SUCCESS;

    if ( @_ >= 1 )
    {
        if ( $_[0] != SUCCESS  and $_[0] != FAILURE and $_[0] != SYSERROR and $_[0] != NO_TIMESTAMP )
        {
            print STDERR "Invalid parameters passed to LogMessage\n";
            exit SYSERROR;
        }
        if ( $_[0] == NO_TIMESTAMP )
        {
            $TimeStampFlag = shift;
        }
        else
        {
            $CurrentErrorCode = shift;
        }
    }

    if ( @_ == 1 and (($_[0] != NO_TIMESTAMP and $_[0] != TIMESTAMP) or defined $TimeStampFlag) )
    {
        print STDERR "Invalid parameters passed to LogMessage\n";
        exit SYSERROR;
    }
    elsif ( @_ == 1 )
    {
        $TimeStampFlag = shift;
    }


    $Stream = ($CurrentErrorCode == SUCCESS) ? STDOUT : STDERR;
    print $Stream $Message . "\n";
    if ( $TimeStampFlag != NO_TIMESTAMP ) 
    {
        my ($Second,$Minute,$Hour,$DayOfMonth,$Month,$Year) = localtime();
        $Year += 1900;
        $Month += 1;
        $Message = sprintf("$Months{$Month} %02d, %04d, %02d:%02d:%02d $Message",
                                  $DayOfMonth, $Year, $Hour, $Minute, $Second);
    }
    open FILE, ">>$LogFile" or Die "Could not open the log file $LogFile for write.\n";
    print FILE $Message . "\n";
    close FILE;
    Die "Could not chmod on $LogFile." unless chmod(0777, "$LogFile") == 1;
    if ( $CurrentErrorCode != SUCCESS )
    {
        $ExitCode = $CurrentErrorCode;
    }
}

sub InspectOutput($$$$$$\$)
{
    my ($TestName, $Ag_Type, $QueryName, $NextLoop, $ExpectedLineCount, $ExpectedContent, $DiffChecked) = @_;
    LogMessage "Checking gsgdatprint output for $QueryName . . .";
    opendir DIR, "." or Die "Could not open current directory for read.";
    my @DirectoryList = readdir DIR;
    closedir DIR;
    my @DataFiles = ();
    foreach my $Item ( @DirectoryList )
    {
        if ( $Item =~ /^[0-9]+.txt$/ ) { push @DataFiles, $Item; }
    }

    if ( @DataFiles == 0 )
    {
        if ( ! $NextLoop )
        {
            LogMessage "Can not locate text files expected from gsgdatprint for $TestName/$Ag_Type/$QueryName.", FAILURE;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{FAILURE}>", NO_TIMESTAMP;
        }
        return;
    }

    foreach my $File (@DataFiles)
    {
        if ( system("${STREAMING}/bin/gdat2ascii $File > Output.txt") )
        {
            ExitWithError "Could not run gdat2ascii for $File.";
        }

        open FILE, "Output.txt" or Die "Could not open Output.txt for read.";
        my $LineCount = 0;
        my ($OutputContent, @OutputContent);
        while (<FILE>)
        {
            s/^[0-9]*\|//;
            push @OutputContent, $_;
            if ( ++$LineCount == $ExpectedLineCount ) { last; }
        }
        close FILE;
        if ( $LineCount == 0 ) {next;}
        ++${$DiffChecked};
        $OutputContent = join "", sort @OutputContent;
        if ( $OutputContent ne $ExpectedContent )
        {
            LogMessage "Exceptions found for type $Ag_Type.", FAILURE;
            LogMessage "Expected output was:", NO_TIMESTAMP;
            LogMessage $ExpectedContent, NO_TIMESTAMP;
            LogMessage "Output obtained is:", NO_TIMESTAMP;
            LogMessage $OutputContent, NO_TIMESTAMP;
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{FAILURE}>", NO_TIMESTAMP;
        }
        else
        {
            LogMessage "No differences found for $TestName/$Ag_Type/$QueryName.";
            LogMessage "Ending: <$TestName/$Ag_Type/$QueryName> <$ErrorStrings{SUCCESS}>", NO_TIMESTAMP;
        }

        last;
    }
}
