#!/user/bin/perl                                                                                             
#######################################################################################                      
# Head Section                                                                                             
# Function:     KPI的IO表                                                                                            
# Create Date:  2014-07-20                                                                                   
# Creator:      DWLUT                                                                                         
# Reviewer:     DWLUT                                                                                       
# Comment:      KPI的IO表
#                                                                        
#--------------------------------------------------------------------------------------                      
#依赖物理表: 
#            
#            
#            
#目标表:                                                                                
#--------------------------------------------------------------------------------------                      
#加载频率:M                                                                                                   
#                                                                                                            
#Modify:(例:)          V1.1 预拆机  20150331                                                                                              
#---------------------------------------------------------------------------------------                     
#注意:                                                                                                       
#1.XXXXXXXXX：XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX。                                                     
#                                                                                                            
######################################################################################### 

  
use strict;     # Declare usINg Perl strict syntax

#####################################################################################################
# ------------ Variable SectiON :DATABSE DEF&INI------------     
my $AUTO_HOME = $ENV{"AUTO_HOME"};
#my $TARGETDB =  "ZJBIC";    
my $TARGETDB =  $ENV{"AUTO_MDATADB"};   
my $WORKDB   =  $ENV{"AUTO_MWORKDB"};
my $SOURCEDB = "BSSDATA";            


my $MAXDATE = $ENV{"AUTO_MAXDATE"};
if ( !defined($MAXDATE) ) {
   $MAXDATE = "30001231";
}

my $MINDATE;
if ( !defined($MINDATE) ) {
   $MINDATE = "19000101";
}
#以下为一些常用变量，请在调用main函数前赋值
#my $LOGON_STR;
#my $LOGON_FILE = "${AUTO_HOME}/etc/DWETL_KPI";
my $CONTROL_FILE;
my $TX_DATE;
my $BILL_MONTH;
my $CUR_MONTH;
my $NEXT_MONTH1;
my $NEXT_MONTH2;
my $CUR_YEAR;

#以下为11个地市取值不同的参数
my $LocalCode;
my $TopCommId;
my $UnknowCommId;
my $Area_Id;
my $Calling_Area_Cd;
my $UnknowTelecomAreaId;
my $LATN_ID;
my $SOURCECODE;
#此处可自定义其他因11个地市而有不同取值的变量，并在后面"if  (  $LocalCode eq ……"处赋值)

my ($hostname, $username, $password);

$hostname = "edatest01";
$username = "dbadmin";
$password = "dbadmin";

my $SCRIPT = "此perl脚本名称";#非必要参数
# ------------ BTEQ function ------------
sub run_vsql_command
{
	my (@de_user_pwd)=@_;
	my $rc = open(VSQL, "| /opt/vertica/bin/vsql -h $hostname -U $username -w $password -e");

  unless ($rc) 
  {
      print "Could not invoke vsql command\n";
      return -1;
  }

# ------ Below are vsql scripts ----------
  print VSQL <<ENDOFINPUT;

\\set AUTOCOMMIT on

\\timing
\\set ON_ERROR_STOP on

COPY BSSDATA.EVT_ORDER_HIST_${LocalCode} FROM '/data/SOURCE/case4.1.5/BSSDATA_EVT_ORDER_HIST_${LocalCode}_*.dat' ON ANY NODE DELIMITER E'\007' DIRECT REJECTMAX 10  NO ESCAPE;

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
}                  #End of VSQL function

sub main
{
   my $ret;

   #open(LOGONFILE_H, "${LOGON_FILE}");
   #$LOGON_STR = <LOGONFILE_H>;
   #close(LOGONFILE_H);

   # Get the decoded logon string
   #$LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;

   # Call bteq command to load data
   $ret = run_vsql_command();
   print "run_vsql_command() = $ret\n";
   return $ret;
}





# ------------ program section ------------

# To see if there is one parameter,
# if there is no parameter, exit program
if ( $#ARGV < 0 ) {
   print "\n";
   print "Usage: $SCRIPT CONTROL_FILE  \n";
   print "Usage: 使用参数 \n";
   print "       CONTROL_FILE  -- 控制文件(SUB_JOBNAMEYYYYMMDD.dir) \n";
   exit(1);
}

# Get the first argument
$CONTROL_FILE = $ARGV[0];
$TX_DATE = substr $CONTROL_FILE,length($CONTROL_FILE)-12,8;
$CUR_MONTH =substr($TX_DATE,0,6);

$CUR_YEAR =substr($TX_DATE,0,4);

$LocalCode = substr $CONTROL_FILE,length($CONTROL_FILE)-14,1;

print "000 $LocalCode 000\n";

#hangzhou
if  (  $LocalCode eq 'A' ) 
    {
    	print "A\n";
    	$Area_Id = "571";
    	$Calling_Area_Cd = "0571";
    	$UnknowTelecomAreaId = 1000;
    	$TopCommId = 2;
    	$UnknowCommId = -71;
        $LATN_ID = 10;
        $SOURCECODE = 71
    }
#huzhou
elsif($LocalCode eq 'B' )
    {
    	print "B\n";
    	$Area_Id = "572";
    	$Calling_Area_Cd = "0572";
    	$UnknowTelecomAreaId = 1100;
    	$TopCommId = 9;
    	$UnknowCommId = -72;
        $LATN_ID = 11;
        $SOURCECODE = 72
     }
#jiaxing
elsif($LocalCode eq 'C' )
    {
    	print "C\n";
    	$Area_Id = "573";
    	$Calling_Area_Cd = "0573";
    	$UnknowTelecomAreaId = 1200;
    	$TopCommId = 5;
    	$UnknowCommId = -73;
    	$LATN_ID = 12;
    	$SOURCECODE = 73
    }    
#ningbo
elsif($LocalCode eq 'D' )
    {
    	print "D\n";
    	$Area_Id = "574";
    	$Calling_Area_Cd = "0574";
    	$UnknowTelecomAreaId = 1300;
    	$TopCommId = 3;
    	$UnknowCommId = -74;
    	$LATN_ID = 13;
    	$SOURCECODE = 74
    }   
#shaoxing    
elsif($LocalCode eq 'E' )
    {
    	print "E\n";
    	$Area_Id = "575";
    	$Calling_Area_Cd = "0575";
    	$UnknowTelecomAreaId = 1400;
    	$TopCommId = 6;
    	$UnknowCommId = -75;
    	$LATN_ID = 14;
    	$SOURCECODE = 75
    }   
#taizhou    
elsif($LocalCode eq 'F' )
    {
    	print "F\n";
    	$Area_Id = "576";
    	$Calling_Area_Cd = "0576";
    	$UnknowTelecomAreaId = 1500;
    	$TopCommId = 8;
    	$UnknowCommId = -76;
    	$LATN_ID = 15;
    	$SOURCECODE = 76
    }       
#wenzhou    
elsif($LocalCode eq 'G' )
    {
    	print "G\n";
    	$Area_Id = "577";
    	$Calling_Area_Cd = "0577";
    	$UnknowTelecomAreaId = 1600;
    	$TopCommId = 4;
    	$UnknowCommId = -77;
    	$LATN_ID = 16;
    	$SOURCECODE = 77
    }     
#lishui  
elsif($LocalCode eq 'H' )
    {
    	print "H\n";
    	$Area_Id = "578";
    	$Calling_Area_Cd = "0578";
    	$UnknowTelecomAreaId = 1700;
    	$TopCommId = 10;
    	$UnknowCommId = -78;
    	$LATN_ID = 17;
    	$SOURCECODE = 78
    }    
#jinhua  
elsif($LocalCode eq 'I' )
    {
    	print "I\n";
    	$Area_Id = "579";
    	$Calling_Area_Cd = "0579";
    	$UnknowTelecomAreaId = 1800;
    	$TopCommId = 7;
    	$UnknowCommId = -79;
    	$LATN_ID = 18;
    	$SOURCECODE = 79
    }      
#zhoushan  
elsif($LocalCode eq 'J' )
    {
    	print "J\n";
    	$Area_Id = "580";
    	$Calling_Area_Cd = "0580";
    	$UnknowTelecomAreaId = 1900;
    	$TopCommId = 11;
    	$UnknowCommId = -80;
    	$LATN_ID = 19;
    	$SOURCECODE = 80
    }    
#quzhou  
elsif($LocalCode eq 'K' )
    {
    	print "K\n";
    	$Area_Id = "570";
    	$Calling_Area_Cd = "0570";
    	$UnknowTelecomAreaId = 2000;
    	$TopCommId = 12;
    	$UnknowCommId = -70;
    	$LATN_ID = 20;
    	$SOURCECODE = 70
    }

 
#账务月计算
    if (substr($TX_DATE, 4, 2) eq "01") 
    {
     	#--如果输入的月份为01月份，则年份减1，月份置为"12"
		$BILL_MONTH = (substr($TX_DATE, 0, 4) - 1)."12";
		
        

	}
	else 
	{
		#--否则,直接月份减"1"
		$BILL_MONTH = substr($TX_DATE, 0, 6) - 1;
		
	}

#下一个月计算
    if (substr($TX_DATE, 4, 2) eq "12") 
    {
     	#--如果输入的月份为01月份，则年份减1，月份置为"12"
		$NEXT_MONTH1 = (substr($TX_DATE, 0, 4) + 1)."01";
		
        

	}
	else 
	{
		#--否则,直接月份加"1"
		$NEXT_MONTH1 = substr($TX_DATE, 0, 6) + 1;
		
	}
#下下一个月计算
    if (substr($NEXT_MONTH1, 4, 2) eq "12") 
    {
     	#--如果输入的月份为01月份，则年份减1，月份置为"12"
		$NEXT_MONTH2 = (substr($NEXT_MONTH1, 0, 4) + 1)."01";
		
        

	}
	else 
	{
		#--否则,直接月份加"1"
		$NEXT_MONTH2 = substr($NEXT_MONTH1, 0, 6) + 1;
		
	}

print "BILL_MONTH  = $BILL_MONTH  \n";
print "CUR_MONTH   = $CUR_MONTH   \n";
print "NEXT_MONTH1 = $NEXT_MONTH1 \n";
print "NEXT_MONTH2 = $NEXT_MONTH2 \n";




open(STDERR, ">&STDOUT");

exit(main());



