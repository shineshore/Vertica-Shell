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
my $SOURCEDB = $ENV{"AUTO_MVIEWDB"};            


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
\\set session resource_pool=pool_4_1_3;

--
CREATE LOCAL TEMP TABLE TMP_3G_TER_${LocalCode}_2 ON COMMIT PRESERVE ROWS
AS
(
SELECT    P1.Asset_Row_Id
          ,P1.Esn_Id
          ,P1.Register_Dt
          ,P1.Accs_Nbr
          ,P1.Equip_Id
          ,P1.Company    --厂商
          ,P1.Term_Model_Id
          ,P1.Ter_Model  --终端型号
          ,P1.SHAOHAO_FLG
          ,p1.TERM_ROW_ID
          ,p1.Intelligent_Flg
          ,p1.TERM_TYPE
          ,P1.NEW_ESN_FLG
FROM      ZJBIC.OFR_TERM_USE_CUR_Z  P1

WHERE     LATN_ID = ${LATN_ID}

)
ORDER BY Asset_Row_Id, Esn_Id, Equip_Id --此表将与TMP_OFR_MAIN_ASSET_X按该字段关联，TMP_OFR_MAIN_ASSET_X为大表，且后面按该字段作分布故此处也用该字段作主索引XMX
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


CREATE LOCAL TEMP TABLE TMP_OFR_MAIN_ASSET_${LocalCode}_1_TMP ON COMMIT PRESERVE ROWS
AS
(
select
P1.ASSET_ROW_ID
,P1.Telecom_Area_Id


FROM BSSDATA.OFR_MAIN_ASSET_HIST_${LocalCode}    P1
INNER JOIN DMN.CAG_COM_STD_PRD_LVL4  P12
ON P1.STD_PRD_LVL4_ID = P12.STD_PRD_LVL4_ID
AND P12.PRD_NAME='CDMA'
INNER join KPI.KPI_ASSET_FLG_${LocalCode}   P3
ON P1.ASSET_ROW_ID = P3.ASSET_ROW_ID
and P3.ON_SERV_FLG = 1
and P3.DATE_CD = TO_DATE('$TX_DATE' ,'YYYYMMDD')  
WHERE P1.Start_Dt <= TO_DATE('$TX_DATE'  ,'YYYYMMDD')  
  and P1.End_Dt   >  TO_DATE('$TX_DATE' ,'YYYYMMDD')  
  and P1.STAT_NAME <> '不活动' 

                --这些WHERE条件都来自资产主资产表的属性，所以上面的排重是不会影响这个筛选结果的，之前的排重使得这个表没有重复数据，下面可以放心使用XMX
)
ORDER BY Asset_Row_Id,Telecom_Area_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;



CREATE LOCAL TEMP TABLE TMP_OFR_MAIN_ASSET_${LocalCode}_1 ON COMMIT PRESERVE ROWS

AS
(
select 
P1.ASSET_ROW_ID
,P1.Telecom_Area_Id
,MAX(CASE WHEN p2.Cdsc_Row_Id IN
            ( '1-18TGP-1'
             ,'1-18TGP-2'
             ,'1-18TGP-3'
             ,'1-18TGP-4'
             ,'1-1AAKH-1'
             ,'1-1AAKH-2'
             ,'1-1AAKH-3'
             ,'1-1ANI7-1'
             ,'1-1CD5L-1'
             ,'1-1CD5L-2'
             ,'1-1CD5L-3'
             ,'1-1CD5L-4'
             ,'1-1CD6D-1'
             ,'1-WCLGAV2'
             ,'1-WCLT15K'
             ,'1-WCMBDHI'
             ,'1-WCMBDHY'
             ,'1-WCN2528'
             ,'1-WCN584K'
             ,'1-WCN5850'
             ,'1-17F80-1'
            )
                 THEN 1 ELSE 0 END) AS Free_Flg              
            ,MAX(CASE WHEN P2.Cdsc_Row_Id='1-17F80-2' THEN 1 ELSE 0 END) AS Corp_Pay_Flg          
            ,MAX(CASE WHEN P2.Cdsc_Row_Id='1-19849-1' THEN 1 ELSE 0 END) AS Test_Flg

FROM TMP_OFR_MAIN_ASSET_${LocalCode}_1_TMP   P1
LEFT join BSSDATA.OFR_ASSET_CDSC_HIST_${LocalCode}   P2
ON P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
and P2.Start_Dt <= TO_DATE('$TX_DATE'  ,'YYYYMMDD') 
and P2.End_Dt   >  TO_DATE('$TX_DATE'  ,'YYYYMMDD')

and P2.STAT_NAME = '使用中'
AND P2.Cdsc_Row_Id IN
            ( '1-18TGP-1'
             ,'1-18TGP-2'
             ,'1-18TGP-3'
             ,'1-18TGP-4'
             ,'1-1AAKH-1'
             ,'1-1AAKH-2'
             ,'1-1AAKH-3'
             ,'1-1ANI7-1'
             ,'1-1CD5L-1'
             ,'1-1CD5L-2'
             ,'1-1CD5L-3'
             ,'1-1CD5L-4'
             ,'1-1CD6D-1'
             ,'1-WCLGAV2'
             ,'1-WCLT15K'
             ,'1-WCMBDHI'
             ,'1-WCMBDHY'
             ,'1-WCN2528'
             ,'1-WCN584K'
             ,'1-WCN5850'
             ,'1-17F80-1'
             ,'1-17F80-2'
             ,'1-19849-1'
            )
GROUP BY P1.ASSET_ROW_ID 
,P1.Telecom_Area_Id 
)
ORDER BY Asset_Row_Id,Telecom_Area_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


CREATE LOCAL TEMP TABLE TMP_3G_TER_${LocalCode} ON COMMIT PRESERVE ROWS

AS
(
select 
P1.*
,P2.Telecom_Area_Id

FROM TMP_3G_TER_${LocalCode}_2   P1
INNER join TMP_OFR_MAIN_ASSET_${LocalCode}_1  P2


ON P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
and P2.Free_Flg = 0
and P2.Corp_Pay_Flg = 0
and P2.Test_Flg = 0
)
ORDER BY Asset_Row_Id, Telecom_Area_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


DELETE FROM KPI.KPI_ASSET_TER_${LocalCode}
WHERE (DATE_CD = TO_DATE('$TX_DATE'  ,'YYYYMMDD') or DATE_CD < TO_DATE('$TX_DATE'  ,'YYYYMMDD') - 62 )

;


INSERT INTO KPI.KPI_ASSET_TER_${LocalCode}

(
Date_Cd
,Esn_Id
,Esn_New_Dt
,Asset_Row_Id
,Accs_Nbr
,Register_Dt
,Telecom_Area_Id
,Equip_Id
,Shaohao_Flg
,Term_Row_Id
,Intelligent_Flg
,Company
,Ter_Model
,Term_Type
,Latn_Id
)
select 

TO_DATE('$TX_DATE'  ,'YYYYMMDD')

,P1.Esn_Id

,CASE WHEN NEW_ESN_FLG = 1 THEN TO_DATE('$TX_DATE' ,'YYYYMMDD') else P3.Esn_New_Dt END 


,P1.Asset_Row_Id
,P1.Accs_Nbr
,P1.Register_Dt

,TO_NUMBER(P1.Telecom_Area_Id)
,P1.Equip_Id
,P1.Shaohao_Flg
,P1.Term_Row_Id
,P1.Intelligent_Flg
,P1.Company
,P1.Ter_Model
,P1.Term_Type

,$LATN_ID

FROM TMP_3G_TER_${LocalCode}   P1
LEFT join KPI.KPI_ASSET_TER_${LocalCode}  p3
ON P1.ESN_ID = P3.ESN_ID

and p3.DATE_CD = TO_DATE('$TX_DATE' ,'YYYYMMDD') - 1 


and LATN_ID = $LATN_ID

;

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



