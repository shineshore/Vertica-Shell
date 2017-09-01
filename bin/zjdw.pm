###############################################################################
# Program: zjdw.pm
# define hash

use strict;
use Time::Local;
#use NET::FTP;
use FindBin;

package ZJDW;
###############################################################################
# variable section
###############################################################################
my $version="1.0";
my %id;

###############################################################################
# function section
###############################################################################
sub showdatetime
{
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time());
    my $current="";
    
    $year=sprintf("%02d",$year+1900);
    $mon =sprintf("%02d",$mon+1);
    $mday=sprintf("%02d",$mday);
    $hour=sprintf("%02d",$hour);
    $min =sprintf("%02d",$min);
    $sec =sprintf("%02d",$sec);
    $current="${year}${mon}${mday}${hour}${min}${sec}";
    return $current;
}

sub calcdate
{
    my($txdate,$period)=@_;
    my($year,$month,$day);
    
    $year =substr($txdate,0,4);
    $month=substr($txdate,4,2);
    $day  =substr($txdate,6,2);

    # Convert string to number in order to cut the prefix zero
    $year +=0;
    $month+=0;
    $day  +=0;

	if($period<0){
		while($period<0){
			if($day==1){
         	$month--;
         	if($month==0){
         		$year--;
					$month=12;
         	}
         	if($month==1||$month==3||$month==5||$month==7||$month==8||$month==10||$month==12){
         		$day=31;
         	}
         	elsif($month==4||$month==6||$month==9||$month==11){
         		$day=30;
         	}
         	elsif($month==2 && ((($year%4)==0 && ($year%100)!=0) || ($year%400)==0)){
					$day=29;
         	}
         	elsif($month==2){
         		$day=28;
         	}
         }else{
         	$day--;
         }
        	$period++;
		}
    }elsif($period>0){
		while($period>0){
			if($month==1||$month==3||$month==5||$month==7||$month==8||$month==10){
				if($day==31){
					$month++;
					$day=1;
				}else{
					$day++;
				}      	    	
			}elsif($month==4||$month==6||$month==9||$month==11){
				if($day==30){
					$month++;
             	$day =1;
            }else{
            	$day++;
            } 
         }elsif($month==12){
				if($day==31){
					$year++;
					$month=1;
             	$day  =1;
            }else{
					$day++;
				}
         }elsif($month==2 && ((($year%4)==0 && ($year%100)!=0) || ($year%400)==0)){
				if($day == 29) {
					$month++;
             	$day=1;
            }else{
            	$day++;
            }
			}elsif($month==2){
				if($day==28){
					$month++;
             	$day=1;
				}else{
					$day++;
            }
			}
         $period--;
      }
    }else{
		return $txdate;
    }
   
    $month=sprintf("%02d",$month);
    $day=sprintf("%02d",$day);
    $txdate="${year}${month}${day}";   
    return $txdate;
}

sub calcmonth
{
    my($txdate,$period)=@_;
    my($year,$month);
   
    $year =substr($txdate,0,4);
    $month=substr($txdate,4,2);

    # Convert string to number in order to cut the prefix zero
    $year +=0;
    $month+=0;

    if($period<0){
      while($period<0){
			if($month==1){
				$year--;
				$month = 12;
         }else{
				$month--;
         }
			$period++;
      }
    }elsif($period>0){
		while($period>0){
         if ($month==12) {
            $year++;
            $month = 1;
         }else{
				$month++;
			}
			$period--;
      }
    }else{
		return substr($txdate,0,6);
    }
	
	$month=sprintf("%02d",$month);
    my $txmonth ="${year}${month}";
    return $txmonth;
}

#--------------------------------------------------------------------------------------
#  子程序:替换函数
#  功能  :将SQL语法中的当前表替换成历史拉链表
#  说明  :将SQL语法中的当前表替换成历史拉链表
#curhis_table('${SOURCEDB1}','FIN_OPRTING_FEE_${localcode1}','${SOURCEDB2}','FIN_OPRTING_FEE_HIST_${localcode2}','Pay_Dt','$tx_date','A')
#${SOURCEDB1}:当前数据库
#${localcode1}:当前地局
#${SOURCEDB2}:历史数据库
#${localcode2}:历史地局
#---------------------------------------------------------------------------------------
# $flag=1代表使用当前表,2代表使用历史表;
sub curhis_table {
	my($sql,$flag)=@_;
	while($sql=~m/curhis_table\(\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\'\)/i){
		if($flag==1){
			if(defined($7)){
				$sql=~s/curhis_table\(\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\'\)/$1.$2 $6/i;
			}
			else{
				$sql=~s/curhis_table\(\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\'\)/$1.$2/i;
			}
		}
		else{
			if(defined($7)){
				$sql=~s/curhis_table\(\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\'\)/(select * from $3.$4 where $5<=cast('$6' as date format 'yyyymmdd') and $5>cast('$6' as date format 'yyyymmdd')) $7/i;
			}
			else{
				$sql=~s/curhis_table\(\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\',\'(\w*)\'\)/(select * from $3.$4 where $5<=cast('$6' as date format 'yyyymmdd') and $5>cast('$6' as date format 'yyyymmdd')) $7/i;
			}
		}
	}
	print $sql;
}

###--------------------------------------------------------------------------------------
###  子程序:参数函数
###  功能  :取出PERL程序相关参数
###  说明  :文件如以下格式
###  调用  :get_inifile2('E:\经验\学习\perl\zjdw.ini','FTP1','passwd');
###  [FTP1]
###  server=134.224.40.82
###  user=db2inst1
###  passwd=123
###---------------------------------------------------------------------------------------
##sub get_inifile2
##{
##	my ($filename,$title,$head)=@_;
##	my ($titleflag,$headflag)=(0,0);
##	my @row;
##	open(FILE,$filename)||die;
##	while(defined(my $line=<FILE>)){
##		chomp($line);
##		if(substr($line,0,1) eq '['){
##			if(uc(substr($line,1,length($line)-2)) eq uc($title)){
##				$titleflag=1;
##				next;
##			}
##		}
##		if($titleflag==1){
##			@row=split(/\=+/,$line);
##			if(uc($row[0]) eq uc($head)){
##				$headflag=1;
##				last;
##			}
##			elsif(substr($line,0,1) eq '['){
##				last;
##			}
##		}
##	}
##	if($titleflag==1 && $headflag==1){
##		return $row[1];
##	}
##	else {
##		return "";
##	}
##}
##
###flag=0上传,flag=1下载
###--------------------------------------------------------------------------------------
###  子程序:FTP函数
###  功能  :FTP文件
###  说明  :文件如以下格式
###  调用  :ftpfile($server,$user,$password,$remotepath,$localpath,$backpath,$filename,$ftplog,$ftpflag);
###         ftpfile('134.224.40.82','db2inst1','db2insttwo','/db2home/db2inst1','c:\Perl1','c:\Perl2','1.txt','c:\Perl1\ftp.log',1);
###---------------------------------------------------------------------------------------
##sub ftpfile{
##	my($server,$user,$password,$remotepath,$localpath,$backpath,$filename,$ftplog,$ftpflag)=@_;
##	my ($ftp,$rc);
##	my $i=0;
##	my $fullfilename=$localpath."\\".$filename;
##	open(FTPLOG,">>$ftplog") || die "Cannot open ",$ftplog;
##	#FTP上传
##	if($ftpflag==0){
##		$ftp=Net::FTP->new("$server",Debug =>0,Passive =>1) || die "Cannot login ",$ftp->message;
##      $rc=$ftp->login("$user","$password");
##      unless($rc){
##  			print "could not log in!\n";
##  			$ftp->quit;
##  			return 0;
##  		}
##      $rc=$ftp->binary();
##      unless($rc){
##  			print "change to bin mode fail!\n";
##  			$ftp->quit;
##  			return 0;
##  		}
##      $rc=$ftp->cwd("$remotepath");
##      unless($rc){
##  			print "change to directory fail!\n";
##  			$ftp->quit;
##  			return 0;
##  		}
##      #$rc=$ftp->put($fullfilename,$filename.'.T');
##      $rc=$ftp->put($fullfilename,$filename);
##      unless($rc){
##      	printf("the command is failed!\n");
##      	$ftp->quit;
##      	return 0;
##      }
##      #$rc=ftp->rename($filename.'.T',$filename);
##      #判断文件传输是否成功
##      my @filest=stat($fullfilename);
##      my @filearrey=$ftp->dir();
##      while($i<=$#filearrey){
##      	if (substr($filearrey[$i],0,1) ne "-" ) {
##      	}
##         else{
##         	chomp($filearrey[$i]);
##	         $filearrey[$i]=~s/ +/ /g;
##	         my @fileattr=split(' ',$filearrey[$i]);
##	         if(uc($filename) eq uc($fileattr[8]) && $filest[7]!=$fileattr[4]){
##    		     	print FTPLOG "put $filename $filest[7] error,deal with next time.\n";
##    		     	$ftp->delete($fileattr[8]);
##    		     	last;
##    			}
##    	      elsif(uc($filename) eq uc($fileattr[8]) && $filest[7]==$fileattr[4]){
##    	      	print FTPLOG "put $filename $filest[7] successful.\n";
##      		   #rename("$fullfilename","$backpath\\$filename")||die $!;
##      		   last;
##    	      }
##      	}
##      	$i++;
##		}
##		$ftp->quit;
##	}
##	#下载
##	elsif($ftpflag==1){
##		$ftp=Net::FTP->new("$server",Debug => 0,Passive =>1) || die "Cannot login ",$ftp->message;
##		$rc=$ftp->login("$user","$password");
##      unless($rc){
##  			print "could not log in!\n";
##  			$ftp->quit;
##  			return 0;
##  		}
##      $rc=$ftp->binary();
##      unless($rc){
##  			print "change to bin mode fail!\n";
##  			$ftp->quit;
##  			return 0;
##  		}
##      $rc=$ftp->cwd("$remotepath");
##      unless($rc){
##  			print "change to directory fail!\n";
##  			$ftp->quit;
##  			return 0;
##  		}
##		chdir("$localpath");
##		$rc=$ftp->get($filename,$filename.'.T');
##      unless($rc){
##      	printf("the command is failed!\n");
##      	$ftp->quit;
##      	return 0;
##      }
##      rename($filename.'.T',$filename);
##      #判断文件传输是否成功
##      my @filest=stat($fullfilename);
##      my @filearrey=$ftp->dir();
##      while($i<=$#filearrey){
##      	if (substr($filearrey[$i],0,1) ne "-" ) {
##      	}
##         else{
##         	chomp($filearrey[$i]);
##	         $filearrey[$i]=~s/ +/ /g;
##	         my @fileattr=split(' ',$filearrey[$i]);
##	         if(uc($filename) eq uc($fileattr[8]) && $filest[7]!=$fileattr[4]){
##    		     	print FTPLOG "get $filename $filest[7] error,deal with next time.\n";
##    		     	last;
##    			}
##    	      elsif(uc($filename) eq uc($fileattr[8]) && $filest[7]==$fileattr[4]){
##    	      	print FTPLOG "get $filename $filest[7] successful.\n";
##      		   last;
##    	      }
##      	}
##      	$i++;
##		}
##		$ftp->quit;
##	}
##	close(FTPLOG);
##	return 1;
##}

%ZJDW::id=(
	"A"=>{
	  Area_Id            =>"571",
      Calling_Area_Cd    =>"0571",
      UnknowTelecomAreaId=>1000,
      TopCommId          =>2,
      Latn_Id            =>10,
      Comm_Id            =>71,
      UnknowCommId       =>'-71',
      Vpn_Grp_Id_1       =>819000,
      Vpn_Grp_Id_2       =>819998,
      Sourcecode         =>71,
      City_id            =>1,
      Z_city_id          =>0,
      Rpt                =>'HZRPT'
	},
	"B"=>{
	  Area_Id            =>"572",
      Calling_Area_Cd    =>"0572",
      UnknowTelecomAreaId=>1100,
      TopCommId          =>9,
      Latn_Id            =>11,
      Comm_Id            =>72,
      UnknowCommId       =>-72,
      Vpn_Grp_Id_1       =>829000,
      Vpn_Grp_Id_2       =>829999,
      Sourcecode         =>72,
      City_id            =>5,
      Z_city_id          =>999,
      Rpt                =>'HUZRPT'
	},
	#jiaxing
	"C"=>{
	  Area_Id            =>"573",
      Calling_Area_Cd    =>"0573",
      UnknowTelecomAreaId=>1200,
      TopCommId          =>5,
      Latn_Id            =>12,
      Comm_Id            =>73,
      UnknowCommId       =>-73,
      Vpn_Grp_Id_1       =>839000,
      Vpn_Grp_Id_2       =>839999,
      Sourcecode         =>73,
      City_id            =>4,
      Z_city_id          =>999,
      Rpt                =>'JXRPT'
	},
	#ningbo
	"D"=>{
      Area_Id            =>"574",
      Calling_Area_Cd    =>"0574",
      UnknowTelecomAreaId=>1300,
      TopCommId          =>3,
      Latn_Id            =>13,
      Comm_Id            =>74,
      UnknowCommId       =>-74,
      Vpn_Grp_Id_1       =>849000,
      Vpn_Grp_Id_2       =>849999,
      Sourcecode         =>74,
      City_id            =>2,
      Z_city_id          =>999,
      Rpt                =>'NBRPT'
	},
	#ningbo
	"E"=>{
      Area_Id            =>"575",
      Calling_Area_Cd    =>"0575",
      UnknowTelecomAreaId=>1400,
      TopCommId          =>32518,
      Latn_Id            =>14,
      Comm_Id            =>75,
      UnknowCommId       =>-75,
      Vpn_Grp_Id_1       =>859000,
      Vpn_Grp_Id_2       =>859999,
      Sourcecode         =>75,
      City_id            =>6,
      Z_city_id          =>999,
      Rpt                =>'SXRPT'
	},
	#taizhou   
	"F"=>{
      Area_Id            =>"576",
      Calling_Area_Cd    =>"0576",
      UnknowTelecomAreaId=>1500,
      TopCommId          =>8,
      Latn_Id            =>15,
      Comm_Id            =>76,
      UnknowCommId       =>-76,
      Vpn_Grp_Id_1       =>869000,
      Vpn_Grp_Id_2       =>869999,
      Sourcecode         =>76,
      City_id            =>9,
      Z_city_id          =>999,
      Rpt                =>'TZRPT'
	},
	#wenzhou      
	"G"=>{
      Area_Id            =>"577",
      Calling_Area_Cd    =>"0577",
      UnknowTelecomAreaId=>1600,
      TopCommId          =>4,
      Latn_Id            =>16,
      Comm_Id            =>77,
      UnknowCommId       =>-77,
      Vpn_Grp_Id_1       =>879000,
      Vpn_Grp_Id_2       =>879999,
      Sourcecode         =>77,
      City_id            =>3,
      Z_city_id          =>999,
      Rpt                =>'WZRPT'
	},
	#lishui      
	"H"=>{
      Area_Id            =>"578",
      Calling_Area_Cd    =>"0578",
      UnknowTelecomAreaId=>1700,
      TopCommId          =>10,
      Latn_Id            =>17,
      Comm_Id            =>78,
      UnknowCommId       =>-78,
      Vpn_Grp_Id_1       =>889000,
      Vpn_Grp_Id_2       =>889999,
      Sourcecode         =>78,
      City_id            =>11,
      Z_city_id          =>999,
      Rpt                =>'LSRPT'
	},
	#jinhua        
	"I"=>{
      Area_Id            =>"579",
      Calling_Area_Cd    =>"0579",
      UnknowTelecomAreaId=>1800,
      TopCommId          =>7,
      Latn_Id            =>18,
      Comm_Id            =>79,
      UnknowCommId       =>-79,
      Vpn_Grp_Id_1       =>899000,
      Vpn_Grp_Id_2       =>899999,
      Sourcecode         =>79,
      City_id            =>7,
      Z_city_id          =>999,
      Rpt                =>'JHRPT'
	},
	#zhoushan        
	"J"=>{
      Area_Id            =>"580",
      Calling_Area_Cd    =>"0580",
      UnknowTelecomAreaId=>1900,
      TopCommId          =>11,
      Latn_Id            =>19,
      Comm_Id            =>80,
      UnknowCommId       =>-80,
      Vpn_Grp_Id_1       =>909000,
      Vpn_Grp_Id_2       =>909999,
      Sourcecode         =>80,
      City_id            =>10,
      Z_city_id          =>999,
      Rpt                =>'ZSRPT'
	},
	#quzhou        
	"K"=>{
      Area_Id            =>"570",
      Calling_Area_Cd    =>"0570",
      UnknowTelecomAreaId=>2000,
      TopCommId          =>12,
      Latn_Id            =>20,
      Comm_Id            =>70,
      UnknowCommId       =>-81,
      Vpn_Grp_Id_1       =>809000,
      Vpn_Grp_Id_2       =>809999,
      Sourcecode         =>70,
      City_id            =>8,
      Z_city_id          =>999,
      Rpt                =>'QZRPT'
	}
);

#--------------------------------------------------------------------------------------
#  子程序:字符替换函数
#  功能  :字符替换
#  说明  :
#  例子 :例如：字符'hello word!'把'or'替换成'or',返回'hello wrod'
#---------------------------------------------------------------------------------------
sub ReplaceStr 
{
    my ($srcStr,$var1,$var2) = @_;
    my $i = 0;
    my $len = length($srcStr);
    my $len1 = length($var1);
    my $len2 = length($var2);
    my $dstStr = "";
    my $k;
    
    if ( $len <= 0 or $len1<= 0 ) {
    	return $srcStr;
    }
    #print "len=$len,len1=$len1,len2=$len2\n";
    while ($i < $len) {
    	$k = 0;
    	while ((substr($srcStr,$i+$k,1) eq substr($var1,$k,1) ) and ( $k < $len1 ) ) { $k++; }
    	#print "k=$k,i=$i\n";
    	if ( $k == $len1 ) {
			$dstStr = $dstStr.$var2;
			$i = $i + $k;
			#print "match once\n";
		} else {
			$dstStr = $dstStr.substr($srcStr,$i,1);
			$i++;
		}
    }
    return $dstStr;
}

#sub ConcatStr
#{
#	my $dstStr = "";
#	chomp(@_);	
#	foreach $array_line(@_){
#		$dstStr = $dstStr.$array_line."  ";
#		}
#	return $dstStr;	
#}
#
#获取用户名和密码
sub GetUserNameAndPwd
{
	my @name_and_pwd = split(/[\s+,\,,;]/,$_[0]);
	return ($name_and_pwd[1],$name_and_pwd[2]);
}
#--------------------------------------------------------------------------------------
#说明：获取文件夹日期
#传入:下一天，执行日期和日、月区分状态获取文件夹日期--分为日和月目录
#---------------------------------------------------------------------------------------
sub getdocdate
{
	my $docday;
	my $bill_month;
	my ($next_day,$tx_date,$loadstatus)=@_;
	if (substr($next_day, 4, 2) eq "01")
     { $bill_month = (substr($next_day, 0, 4) - 1)."12";
     }
    else
     { $bill_month = substr($next_day, 0, 6) - 1;
     }
    
    if ( $loadstatus eq 'D')
     {   $docday = $tx_date;
     }
    else     
     {
         $docday = $bill_month;
     }
    return ($docday,$bill_month);
}

#--------------------------------------------------------------------------------------
#说明：取半年包到期月份
#传入:传入当前天和到期月份，返回半年包到期月份
#---------------------------------------------------------------------------------------
sub calcmonth2
{   
    my $M2;
    my ($TX_DATE,$M1)=@_;
    if (substr($TX_DATE, 4, 2) > "06") 
    {
     	#--如果输入的月份大于06月份，则月份减6
		$M2 ="0".($M1 - 6)       

	}
	else #--如果输入的月份小于06月份，则月份加6
	{
		#--如果输入的月份小于03月份，则月份加6置为"0".
		if($M1>"03")  {$M2 = $M1 + 6}
		 else { $M2 = "0".($M1 + 6)}		
	}  
      return $M2;
}

sub getPlFileName
{
	
	$0 = "stealth";
    return "$FindBin::Bin/$FindBin::Script";
}





# Don't remove the below line, otherwise, the other perl program
# which require this file will be terminated,
# it has to be true value at the last line.
1;

__END__
