#! /usr/bin/perl -w

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

sub LogMessage;

$| = 1;

use constant { SUCCESS => 0, FAILURE => 1, SYSERROR => 2, TIMESTAMP => 3, NO_TIMESTAMP => 4 };
%ErrorStrings = ( SUCCESS => "Success", FAILURE => "Failure", SYSERROR => "SystemError",
                  0 => "Success", 1 => "Failure", 2 => "SystemError" );
$ID=`id -un`;
chomp $ID;
$PWD = cwd();
($STREAMING) = $PWD =~ /^(.*\/STREAMING\b)/;
$STREAMING="$ENV{HOME}/STREAMING" if ( ! defined $STREAMING );
if ( ! -d "$STREAMING/test" )
{
    LogMessage( "Could not locate STREAMING directory.", SYSERROR );
    exit SYSERROR;    
}
$STREAMING="$STREAMING/test";

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

my $LogFile;
{
    my ($Second,$Minute,$Hour,$DayOfMonth,$Month,$Year) = localtime();
    $Year += 1900;
    $Month += 1;
    $LogFile= sprintf("$STREAMING/test_results_%04d-%02d-%02d.txt",
                      $Year , $Month, $DayOfMonth);
}

($Script) = $0 =~ /^.*?\/?([^\/]+)$/;

$ExitCode = SUCCESS;
LogMessage "$0 running with ProcessID $$.";

chdir "$STREAMING" or die "Could not change directory to $STREAMING: $!";
if ( $RC = system("$STREAMING/clean_coverage_files.pl") )
{
    LogMessage "System error:$STREAMING/clean_coverage_files.pl returned $RC.", SYSERROR;
}

opendir DIR, "." or die "Could not open current directory for read.";
@DirectoryList = readdir DIR;
closedir DIR;
@TestCases=();

foreach my $Item ( @DirectoryList )
{
    if ( $Item eq "." or $Item eq ".." ) { next; }
    if ( -d $Item and ! -l $Item )
    {
        push @TestCases, $Item;
    }
}

if ( @ARGV == 1 and -d $ARGV[0] )
{
    @TestCases=@ARGV;
}
elsif ( @ARGV > 1 or (@ARGV == 1 and ! -d $ARGV[0]) )
{
    LogMessage "Invalid arguments @ARGV.", SYSERROR;
    exit SYSERROR;
}

foreach my $TestCase ( @TestCases )
{
    if ( ! -f "$TestCase/run_test.pl" or ! -x "$TestCase/run_test.pl" )
    {
        LogMessage "File $TestCase/run_test.pl can not be located.", SYSERROR;
        next;
    }
    my $RC = system("$TestCase/run_test.pl") >> 8;
    if ( $RC > $ExitCode and defined $ErrorStrings{$RC} ) { $ExitCode = $RC; }
    elsif ( $RC > SYSERROR ) { $ExitCode = SYSERROR; }
    my $ErrorDetails = defined $ErrorStrings{$RC} ? $ErrorStrings{$RC} : $RC;
    LogMessage("$TestCase completed with status $ErrorDetails");
    LogMessage "Ending: <$TestCase> <$ErrorDetails>", NO_TIMESTAMP;
}

if ( $RC = system("$STREAMING/extract_coverage_results.pl") )
{
    LogMessage "System error:$STREAMING/extract_coverage_results.pl returned $RC.", SYSERROR;
}

if ( $ExitCode > SYSERROR )
{
    LogMessage "$STREAMING/$Script Warning: Unexpected ExitCode $ExitCode is treated as $ErrorStrings{SYSERROR}.";
    $ExitCode = SYSERROR;
}


LogMessage "$STREAMING/$Script exiting with return code $ErrorStrings{$ExitCode}";
exit $ExitCode;


sub LogMessage
{
    my $TimeStampFlag = TIMESTAMP;
    if  ( @_ > 3 or !defined $_[0] or $_[0] eq "" )
    {
        print STDERR "Invalid parameters passed to LogMessage()\n";
        exit FAILURE;
    }
    my $Message = shift;
    chomp $Message;
    my $CurrentErrorCode = SUCCESS;

    if ( @_ >= 1 )
    {
        if ( $_[0] != SUCCESS  and $_[0] != FAILURE and $_[0] != SYSERROR and $_[0] != NO_TIMESTAMP )
        {
            print STDERR "Invalid parameters passed to LogMessage\n";
            exit FAILURE;
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
        exit FAILURE;
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
    open FILE, ">>$LogFile" or die "Could not open the log file $LogFile for write.\n";
    print FILE $Message . "\n";
    close FILE;
    if ( $CurrentErrorCode != SUCCESS )
    {
        $ExitCode = $CurrentErrorCode;
    }
}
