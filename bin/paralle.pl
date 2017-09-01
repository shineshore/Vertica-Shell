#!/usr/bin/perl
###############################################################################
# 功能描述： 并行执行脚本
# 参数说明： 控制文件名称（全路径）
# 开发日期： 2017/07/13
###############################################################################
use strict; 
use Time::Local;

my $CFG_FILE;
my $AUTO_HOME = $ENV{"AUTO_HOME"};
my @jobs;
my @con_name;
my $logdir="$AUTO_HOME/logs";

#####################################################################################################
# showTime section
sub showTime
{
   my ($output) = @_;

   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${hour}:${min}:${sec}";

   if ( defined($output) ) {
      print $output ("[$current] ");
   }
   else {
      print "[$current] ";
   }
}

#####################################################################################################
# cfgParser section
sub cfgParser
{
	my ($cfgfile) = @_;
	
	unless ( open (CFGFILE,"$cfgfile")){
		showTime(); print "Fail to open $cfgfile \n"; 
	}

	my $cfg ;
	
	@con_name=$cfgfile;
	for $cfg ( <CFGFILE> ){
		chomp($cfg);
		if ( substr($cfg,0,1) == "#"){next}
		push(@jobs,($cfg));
	}
	
	close(CFGFILE);
	
	return 0;
}

#####################################################################################################
# runJobs section
sub runJobs
{

	my @pid;
	for (my $i=0;$i<= $#jobs;$i++){
		my($jobid,$jobtype,$jobpath,$job,$cnt,$parameter) = split ('\|',$jobs[$i]);
		chomp($parameter);
		
		
		for (my $j=1;$j<=$cnt;$j++){
			
			my $param=$cnt*5;
			my $cmd = system("$jobtype $jobpath/$job $parameter > $logdir/$jobid.$param.${job}.${j}.log 2>&1 &");
			showTime(); print "[$cmd] $jobtype $jobpath/$job $parameter \n";
		}
		
	}
}

#####################################################################################################
# main section
sub main()
{
	if (-f "$CFG_FILE"){
		showTime(); print "Config file $CFG_FILE exist. \n";
	}else{
		showTime(); print "Config file $CFG_FILE is not exist. \n";
		return 12;
	}
	
	my $ret_code;
	
	$ret_code = cfgParser($CFG_FILE);
	
	$ret_code = runJobs();
	
	return 0;
}

#####################################################################################################
# program section

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   exit(1);
}

$CFG_FILE = $ARGV[0];

open(STDERR, ">&STDOUT");

my $rc = main();

showTime(); print "main() = $rc \n";

exit $rc;


print "main() = $rc \n";

exit $rc;
