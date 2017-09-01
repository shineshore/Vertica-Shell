#!/usr/bin/perl

use strict;     # Declare usINg Perl strict syntax
use Time::Local;

my $interval = 30;	#循环间等待的秒数
my $sql = "Insert into item2 select * from item;commit;";	#循环执行的sql语句
my ($hostname, $username, $password);

$hostname = "edatest01";
$username = "dbadmin";
$password = "dbadmin";

##------------ main function ------------
sub main
{
   ###在主函数中直接调用下面的程序，不用修改
   my $ret ;
   print "------------------------RUNNING VSQL START---------------------------\n";  
   while(1){   
   	$ret=run_vsql_command();
   
   	print "rc=$ret\n";
   	sleep($interval);
   }
   print "------------------------RUNNING VSQL END-----------------------------\n";  
   return $ret;
}

open(STDERR, ">&STDOUT");
my $rc = exit(main());

sub run_vsql_command
{
	my (@de_user_pwd)=@_;
	my $rc = open(VSQL, "| /opt/vertica/bin/vsql -h $hostname -U $username -w $password");

  unless ($rc) 
  {
      print "Could not invoke vsql command\n";
      return -1;
  }

# ------ Below are vsql scripts ----------
  print VSQL <<ENDOFINPUT;

--\\set ON_ERROR_STOP on    --该语法的作用是，在ON_ERROR_STOP属性打开后，下面执行的sql语句一旦报错就退出vsql,下面的sql不会执行，知道遇到关闭该属性的语句,一旦遇到错误，最终vsql的返回值是3,否则是0,一般建议最后一个sql前一定要加上该属性,否则整个脚本最后的返回码肯定是0

---\\set ON_ERROR_STOP off   --该语法的作用是，在ON_ERROR_STOP属性关闭后，下面执行的sql语句及时遇到报错也不退出vsql，最终vsql的返回值是0因此要灵活应用这两个开关

\\set AUTOCOMMIT on

\\timing
\\set ON_ERROR_STOP on
set search_path=tpc;

$sql 



\\q
ENDOFINPUT

close(VSQL);

  
  my $RET_CODE = $?>>8 ;

  if ( $RET_CODE != 0 ) {
      return 1;
  }
  else {
      return 0;
  }
}
