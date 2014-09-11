#! /usr/bin/perl
use Cwd;

$curr_path = getcwd();
if($curr_path =~ /^(.*\/STREAMING)\//){
        $prefix = $1;
}else{
        print "didn't find prefix.\n";
        exit(1);
}

$glob_str = "$prefix/src/ftacmp/*.gcda";
@gcda_files = sort {$a cmp $b} glob($glob_str);

unlink @gcda_files;

