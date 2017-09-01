#!/usr/bin/perl	
use strict;     # Declare usINg Perl strict syntax
#use Net::FTP;
use DBI;
use Time::Local;
#use Win32API::File;
#use Net::Telnet;

if($#ARGV<0){
   print "\n";
   #print "usage: $script control_file\n";
   print "usage: 使用参数\n";
   print "Control_File--控制文件(SUB_JOBNAMEYYYYMMDD.dir)\n";
   exit(1);
}

# databse def&ini
my $Auto_Home          =$ENV{"AUTO_HOME"};
my $Targetdb           ="PRTDATA";#PRTDATA
my $Tempdb             ="MARTTEMP";
my $Bicviewdb           ="BSSVIEW";   
my $Pdmdb              ="BSSDATA"; 
my $Bicviewdb          ="BICVIEW_Z";#BICVIEW_Z
my $Appviewdb          ="APPVIEW";
my $Dimensiondb        ="DMNVIEW";
my $Dmndb              ="DMN";
my $Bicdb              ="ZJBIC";
my $Testdb             ="MODELTEST"; #测试库
my $Stagedb            ="STAGE";
my $Hemsdb             =$ENV{"HEMS"}; 

unshift(@INC, "$Auto_Home/bin");
require zjdw;    
#require zjdw_td;
#require zjdw_db2;

# Get the first argument
my $Control_File       =$ARGV[0];
my $Tx_Today           =substr($Control_File,length($Control_File)-12,8);#当天为"YYYYMMDD" char类型
my $LocalCode          =substr($Control_File,length($Control_File)-14,1);#本地网标识
my $Db2_Logon_File        =$Auto_Home."/etc/LOGON_117_DB2";#一般配置 DB2可在这里修改加密文件  LOGON_PRT_DB2
#my $Db2_Logon_Str         =ZJDW_DB2::getconnectlogonstr($Auto_Home,$Db2_Logon_File); 

#老时间参数
my $TX_DATE            =substr($Tx_Today,0,4).'-'.substr($Tx_Today,4,2).'-'.substr($Tx_Today,6,2);#当天
my $CUR_MONTH          =substr($Tx_Today,0,4).substr($Tx_Today,4,2);#当前月
my $CUR_YEAR           =substr($Tx_Today,0,4);#当前年
my $STMT_DATE          =substr($Tx_Today, 0, 4).'-'.substr($Tx_Today,4,2).'-'.'01';#出帐日期
my $STMT_MONTH         =substr($Tx_Today, 4, 2);  #出帐月份
my $TX_N_DATE          =substr($Tx_Today,0,4).substr($Tx_Today,4,2).substr($Tx_Today,6,2);#当天
#新增月份参数
my $BIL_MONTH         =substr(ZJDW::calcmonth($Tx_Today,-1),0,4).substr(ZJDW::calcmonth($Tx_Today,-1),4,2);#上个月六位
my $BIL_MONTH1        =substr(ZJDW::calcmonth($Tx_Today,-2),0,4).substr(ZJDW::calcmonth($Tx_Today,-2),4,2);#上上个月
my $BIL_MONTH2        =substr(ZJDW::calcmonth($Tx_Today,-3),0,4).substr(ZJDW::calcmonth($Tx_Today,-3),4,2);#上三个月
my $NEX_MONTH1        =substr(ZJDW::calcmonth($Tx_Today,1),0,4).substr(ZJDW::calcmonth($Tx_Today,1),4,2);#下个月
my $NEX_MONTH2        =substr(ZJDW::calcmonth($Tx_Today,2),0,4).substr(ZJDW::calcmonth($Tx_Today,2),4,2);#下个月
my $NEX_MONTH3        =substr(ZJDW::calcmonth($Tx_Today,3),0,4).substr(ZJDW::calcmonth($Tx_Today,3),4,2);#下个月
#原月份参数（请勿删除，后面有引用）
my $BILL_MONTH         =substr(ZJDW::calcmonth($Tx_Today,-1),0,4).substr(ZJDW::calcmonth($Tx_Today,-1),4,2);#上个月
my $BILL_MONTH1        =substr(ZJDW::calcmonth($Tx_Today,-2),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,-2),4,2);#上上个月
my $BILL_MONTH2        =substr(ZJDW::calcmonth($Tx_Today,-3),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,-3),4,2);#上三个月
my $NEXT_MONTH1        =substr(ZJDW::calcmonth($Tx_Today,1),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,1),4,2);#下个月
my $NEXT_MONTH2        =substr(ZJDW::calcmonth($Tx_Today,2),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,2),4,2);#下个月
my $NEXT_MONTH3        =substr(ZJDW::calcmonth($Tx_Today,3),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,3),4,2);#下个月

#时间参数
my $Cur_Date_Yyyy_Mm_Dd           =substr($Tx_Today,0,4).'-'.substr($Tx_Today,4,2).'-'.substr($Tx_Today,6,2);#当天
my $Next_Day_Yyyy_Mm_Dd           =substr(ZJDW::calcdate($Tx_Today,1),0,4).'-'.substr(ZJDW::calcdate($Tx_Today,1),4,2).'-'.substr(ZJDW::calcdate($Tx_Today,1),6,2);#明天
my $Next_Day1_Yyyy_Mm_Dd          =substr(ZJDW::calcdate($Tx_Today,2),0,4).'-'.substr(ZJDW::calcdate($Tx_Today,2),4,2).'-'.substr(ZJDW::calcdate($Tx_Today,2),6,2);#后天
my $Next_Day2_Yyyy_Mm_Dd          =substr(ZJDW::calcdate($Tx_Today,3),0,4).'-'.substr(ZJDW::calcdate($Tx_Today,3),4,2).'-'.substr(ZJDW::calcdate($Tx_Today,3),6,2);#大后天
my $Begfore_Day1_Yyyy_Mm_Dd       =substr(ZJDW::calcdate($Tx_Today,-1),0,4).'-'.substr(ZJDW::calcdate($Tx_Today,-1),4,2).'-'.substr(ZJDW::calcdate($Tx_Today,-1),6,2);#昨天
my $Begfore_Day1_Yyyy_Mm_N_Dd       =substr(ZJDW::calcdate($Tx_Today,-1),0,4).substr(ZJDW::calcdate($Tx_Today,-1),4,2).substr(ZJDW::calcdate($Tx_Today,-1),6,2);#NEW昨天

my $Begfore_Day2_Yyyy_Mm_Dd       =substr(ZJDW::calcdate($Tx_Today,-2),0,4).'-'.substr(ZJDW::calcdate($Tx_Today,-2),4,2).'-'.substr(ZJDW::calcdate($Tx_Today,-2),6,2);#前天
my $Begfore_Day2_Yyyy_Mm_N_Dd       =substr(ZJDW::calcdate($Tx_Today,-2),0,4).substr(ZJDW::calcdate($Tx_Today,-2),4,2).substr(ZJDW::calcdate($Tx_Today,-2),6,2);#NEW前天
my $Begfore_Day3_Yyyy_Mm_Dd       =substr(ZJDW::calcdate($Tx_Today,-3),0,4).'-'.substr(ZJDW::calcdate($Tx_Today,-3),4,2).'-'.substr(ZJDW::calcdate($Tx_Today,-3),6,2);#大前天
my $Cur_Month_Yyyy_Mm             =substr($Tx_Today,0,4).'-'.substr($Tx_Today,4,2);#当前月
my $Next_Month1_Yyyy_Mm           =substr(ZJDW::calcmonth($Tx_Today,1),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,1),4,2);#下个月
my $Next_Month2_Yyyy_Mm           =substr(ZJDW::calcmonth($Tx_Today,2),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,2),4,2);#下下个月
my $Next_Month3_Yyyy_Mm           =substr(ZJDW::calcmonth($Tx_Today,3),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,3),4,2);#下三个月
my $Before_Month_Yyyy_Mm          =substr(ZJDW::calcmonth($Tx_Today,-1),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,-1),4,2);#上个月
my $Before_Month1_Yyyy_Mm         =substr(ZJDW::calcmonth($Tx_Today,-2),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,-2),4,2);#上上个月
my $Before_Month2_Yyyy_Mm         =substr(ZJDW::calcmonth($Tx_Today,-3),0,4).'-'.substr(ZJDW::calcmonth($Tx_Today,-3),4,2);#上三个月
my $Cur_Year_Yyyy                 =substr($Tx_Today,0,4);#当前年
my $Next_Year1_Yyyy               =$Cur_Year_Yyyy+1;#明年
my $Next_Year2_Yyyy               =$Cur_Year_Yyyy+2;#后年
my $Before_Year1_Yyyy             =$Cur_Year_Yyyy-1;#去年
my $Before_Year2_Yyyy             =$Cur_Year_Yyyy-2;#前年
my $Stmt_Date_Yyyy_Mm_Dd          =substr($Tx_Today, 0, 4).'-'.substr($Tx_Today,4,2).'-'.'01';#出帐日期
my $Next_First_Day_Yyyy_Mm_Dd     =$Next_Month1_Yyyy_Mm.'-'.'01';   #下月第一天 
my $Next_First_Day1_Yyyy_Mm_Dd    =$Next_Month2_Yyyy_Mm.'-'.'01';	 #下下月第一天
my $Next_First_Day2_Yyyy_Mm_Dd    =$Next_Month3_Yyyy_Mm.'-'.'01';	 #下三个月第一天
my $Before_First_Day_Yyyy_Mm_Dd   =$Before_Month_Yyyy_Mm.'-'.'01';    #上个月第一天
my $Before_First_Day1_Yyyy_Mm_Dd  =$Before_Month1_Yyyy_Mm.'-'.'01';   #上上个月第一天
my $Before_First_Day2_Yyyy_Mm_Dd  =$Before_Month2_Yyyy_Mm.'-'.'01';   #上三个月第一天
my $Cur_Month_Mm                  =substr($Tx_Today, 4, 2);  #出帐月份
my $Next_Month_Mm                 =substr(ZJDW::calcmonth($Tx_Today,1),4,2); #下个月份
my $Next_Month1_Mm                =substr(ZJDW::calcmonth($Tx_Today,2),4,2); #下下个月份
my $Next_Month2_Mm                =substr(ZJDW::calcmonth($Tx_Today,3),4,2); #下三个月份
my $Before_Month_Mm               =substr(ZJDW::calcmonth($Tx_Today,-1),4,2); #上个月份
my $Before_Month1_Mm              =substr(ZJDW::calcmonth($Tx_Today,-2),4,2); #上上个月份
my $Before_Month1_Mm              =substr(ZJDW::calcmonth($Tx_Today,-3),4,2); #上三个月份
my $Half_Month_Yyyy_Mm_Dd         =ZJDW::calcmonth2($Tx_Today,$Cur_Month_Mm);#半年包到期月份

# 相关参数
my $Area_Id            =$ZJDW::id{$LocalCode}{Area_Id};  #营业区
my $Calling_Area_Cd    =$ZJDW::id{$LocalCode}{Calling_Area_Cd};#主叫营业区
my $UnknowTelecomAreaId=$ZJDW::id{$LocalCode}{UnknowTelecomAreaId};#默认电信营业区
my $TopCommId          =$ZJDW::id{$LocalCode}{TopCommId};#地域编码
my $Latn_Id            =$ZJDW::id{$LocalCode}{Latn_Id};#本地网
my $Jt_Latn_Id         =$ZJDW::id{$LocalCode}{Jt_Latn_Id};#集团本地网
my $Latn_Name					 =$ZJDW::id{$LocalCode}{Latn_Name}.'市分公司';#本地网名称
my $Comm_Id            =$ZJDW::id{$LocalCode}{Comm_Id};#地域
my $UnknowCommId       =$ZJDW::id{$LocalCode}{UnknowCommId};#默认地域编码
my $Vpn_Grp_Id_1       =$ZJDW::id{$LocalCode}{Vpn_Grp_Id_1};#VPN群号id1
my $Vpn_Grp_Id_2       =$ZJDW::id{$LocalCode}{Vpn_Grp_Id_2};#VPN群号id2
my $Sourcecode         =$ZJDW::id{$LocalCode}{Sourcecode};#source标识
my $City_Id            =$ZJDW::id{$LocalCode}{City_id};#城市id
my $Z_City_Id          =$ZJDW::id{$LocalCode}{Z_city_id};
my $Rpt                =$ZJDW::id{$LocalCode}{Rpt};#本地库替换名
my $Script             ="";#可以在这里写脚本名称
my $Exporthome         ="D:\\ETL\\exp\\zsy";#导出文件目录定义     
my $Datafile           ="zsy_export";       #导出文件名定义  

my $MAXDATE=$ENV{"AUTO_MAXDATE"};
if(!defined($MAXDATE)){
	$MAXDATE = "3000-12-31";
}

my $MINDATE=$ENV{"AUTO_MINDATE"};
if(!defined($MINDATE)){
	$MINDATE="1900-01-01";
}

my $NULLDATE = $ENV{"AUTO_NULLDATE"};
if ( !defined($NULLDATE) ) {
    $NULLDATE = "1900-01-02";
}

my $ILLDATE = $ENV{"AUTO_ILLDATE"};
if ( !defined($ILLDATE) ) {
    $ILLDATE = "1900-01-03";
}




my $sql=<<INPUT

CREATE LOCAL TEMP TABLE OFR_PROM_BSI_HIST_Z ON COMMIT PRESERVE ROWS
AS(
	SELECT  P1.PROM_ROW_ID
		 ,P1.PROM_TYPE_NAME
							,P1.PROM_EFF_DT
							,P1.PROM_EXP_DT
							,P1.PROM_INTEG_ID
							,P1.PROM_ASSET_INTEG_ID
							,P1.STAT_NAME
							,P1.LATN_ID
							,P1.PROM_INSTANT_ROW_ID
							,P1.START_DT
							,P1.HQ_OFFER_INST_ID                           -----------V1.1
 		  FROM    STAGE_2.OFR_PROM_BSI_HIST_Z  P1
 		--/* haoyf */ WHERE    P1.START_DT = DATE('$TX_DATE')
 		 --WHERE     P1.START_DT = DATE('20170101')
		  WHERE    P1.START_DT = DATE('$TX_DATE')
 	           AND    P1.END_DT=DATE('3000-12-31')
 		   --/* haoyf */ AND    P1.LATN_ID = $Latn_Id
		   --AND    P1.LATN_ID = 10
		   AND    P1.LATN_ID = $Latn_Id
 		   AND  2=1    -- ADD BY HAOYF
)
SEGMENTED BY  HASH(PROM_INTEG_ID) ALL NODES KSAFE 0
;


INSERT INTO OFR_PROM_BSI_HIST_Z
SELECT  P1.PROM_ROW_ID
							,P1.PROM_TYPE_NAME
							,P1.PROM_EFF_DT
							,P1.PROM_EXP_DT
							,P1.PROM_INTEG_ID
							,P1.PROM_ASSET_INTEG_ID
							,P1.STAT_NAME
							,P1.LATN_ID
							,P1.PROM_INSTANT_ROW_ID
							,P1.START_DT
							,P1.HQ_OFFER_INST_ID                         -----------V1.1
 			FROM    STAGE_2.OFR_PROM_BSI_HIST_Z  P1
 		--/* haoyf */ WHERE    P1.START_DT = DATE('$TX_DATE')
 		      --WHERE    P1.START_DT = DATE('20170101')
			  WHERE    P1.START_DT = DATE('$TX_DATE')
 			and P1.END_DT=DATE('3000-12-31')
 		  --/* haoyf */ AND    P1.LATN_ID = $Latn_Id
 		        --AND    P1.LATN_ID = 10
				AND    P1.LATN_ID = $Latn_Id
;


--------【1】脚本数据加载方式采用增量处理的方式
----【1.1】获取OFR_PROM_HIST_Z当日订单信息增量数据
--/* haoyf */ CREATE LOCAL TEMPORARY TABLE PROM_ORDER_INFO_${LocalCode}
CREATE LOCAL TEMP TABLE PROM_ORDER_INFO_${LocalCode}  ON COMMIT PRESERVE ROWS
AS ( SELECT
	    *
        FROM
        (
        SELECT   COALESCE(P2.ORDER_PROM_ROW_ID,'-1') AS ORDER_PROM_ROW_ID
                  ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.HQ_OFFER_INST_ID                                                    -----------V1.1
                            ,row_number () OVER( PARTITION BY P1.PROM_INSTANT_ROW_ID ORDER BY P2.Apply_Time  DESC )  ROW_NUM
                            
             FROM    OFR_PROM_BSI_HIST_Z  P1
INNER JOIN    STAGE_2.EVT_ORDER_PROM_HIST_Z P2
        ON    P1.PROM_INTEG_ID = P2.PROM_INTEG_ID
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
       --AND    P2.LATN_ID = 10
	   AND    P2.LATN_ID = $Latn_Id
       --/* haoyf */ AND    P2.ETL_DT >= (DATE('$TX_DATE') - 7 DAY)
       --AND    P2.ETL_DT >= (DATE('20170101') - 7)
	   AND    P2.ETL_DT >= (DATE('$TX_DATE') - 7)
       AND    P2.STAT_NAME = '添加'
         ) AS T1
          WHERE ROW_NUM = 1
          AND 2=1  /* ADD BY HAOYF */       
)
ORDER BY PROM_INTEG_ID, ORDER_PROM_ROW_ID
SEGMENTED BY HASH(ORDER_PROM_ROW_ID) ALL NODES KSAFE 0
;


--/* haoyf */INSERT INTO PROM_ORDER_INFO_${LocalCode}
INSERT INTO PROM_ORDER_INFO_${LocalCode}
                SELECT
                            *
        FROM
        (
        SELECT
                            COALESCE(P2.ORDER_PROM_ROW_ID,'-1') AS ORDER_PROM_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.HQ_OFFER_INST_ID                                                           -----------V1.1
                            ,ROW_NUMBER() OVER(PARTITION BY P1.PROM_INSTANT_ROW_ID ORDER BY P2.Apply_Time  DESC    )  ROW_NUM
             FROM    OFR_PROM_BSI_HIST_Z  P1
INNER JOIN    STAGE_2.EVT_ORDER_PROM_HIST_Z P2
        ON    P1.PROM_INTEG_ID = P2.PROM_INTEG_ID
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
	   --AND    P2.LATN_ID = 10
	   AND    P2.LATN_ID = $Latn_Id
       --/* haoyf */AND    P2.ETL_DT >= (DATE('$TX_DATE') - 7 DAY)
	   AND    P2.ETL_DT >= (DATE('$TX_DATE') - 7)
       AND    P2.STAT_NAME = '添加'
         ) AS T1
          WHERE ROW_NUM = 1  
;

--/* haoyf */DECLARE GLOBAL TEMPORARY TABLE EVT_ORDI_HIST_${LocalCode}
CREATE LOCAL TEMP TABLE EVT_ORDI_HIST_${LocalCode}  ON COMMIT PRESERVE ROWS
AS(
    SELECT  P2.ORDER_ITEM_ROW_ID
                            ,P2.ASSET_INTEG_ID
                            ,P2.ORDER_ROW_ID
                            ,P2.STAT_NAME AS ORDI_STAT_NAME
                            ,P2.ETL_DT
                            ,P2.CPL_DT
                            ,P2.Sales_Emp_Id
                            ,P2.Prom_Flg
                       ,P2.action_name
       --/* haoyf */FROM    STAGE_2.EVT_ORDI_HIST_${LocalCode}  P2
	   FROM    STAGE_2.EVT_ORDI_HIST_${LocalCode}  P2
         WHERE     P2.STAT_NAME = '完成'
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
	   AND    P2.LATN_ID = $Latn_Id
	   AND 2=1 /* ADD BY HAOYF */
)
ORDER BY ORDER_ITEM_ROW_ID, ASSET_INTEG_ID
SEGMENTED BY HASH (ORDER_ITEM_ROW_ID) ALL NODES KSAFE 0
;


--/* haoyf */INSERT intO EVT_ORDI_HIST_${LocalCode}
INSERT intO EVT_ORDI_HIST_${LocalCode}
SELECT  P2.ORDER_ITEM_ROW_ID
                            ,P2.ASSET_INTEG_ID
                            ,P2.ORDER_ROW_ID
                            ,P2.STAT_NAME AS ORDI_STAT_NAME
                            ,P2.ETL_DT
                            ,P2.CPL_DT
                            ,P2.Sales_Emp_Id
                            ,P2.Prom_Flg
                       ,P2.action_name
        --/* haoyf */     FROM    STAGE_2.EVT_ORDI_HIST_${LocalCode}  P2
		FROM    STAGE_2.EVT_ORDI_HIST_${LocalCode}  P2
         WHERE     P2.STAT_NAME = '完成'
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
	   --AND    P2.LATN_ID = 10
	   AND    P2.LATN_ID = $Latn_Id
;

CREATE LOCAL TEMP TABLE PROM_TYPE_MAIN  ON COMMIT PRESERVE ROWS
AS(
        SELECT
                            P1.ORDER_PROM_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
                            ,P2.ORDER_ITEM_ROW_ID
                            ,P2.ORDER_ROW_ID
                            ,P2.ORDI_STAT_NAME
                            ,P2.ETL_DT
                            ,P2.CPL_DT
                            ,P2.Sales_Emp_Id
 --/* haoyf */         FROM        PROM_ORDER_INFO_${LocalCode}                 P1
       FROM        PROM_ORDER_INFO_${LocalCode}                P1
 --/* haoyf */LEFT JOIN    EVT_ORDI_HIST_${LocalCode}         P2
 LEFT JOIN    EVT_ORDI_HIST_${LocalCode}         P2
        ON        P1.PROM_ASSET_INTEG_ID = P2.ASSET_INTEG_ID
       AND    P2.Prom_Flg = 'Y'
       and    p2.action_name = '添加'
     WHERE    P1.PROM_TYPE_NAME IN ('组合套餐','帐户级销售品','单一套餐')
	   AND    2=1 /* ADD BY HAOYF*/
)
ORDER BY ORDER_PROM_ROW_ID,PROM_ROW_ID,PROM_TYPE_NAME
SEGMENTED BY HASH(ORDER_PROM_ROW_ID) ALL NODES KSAFE 0
;

INSERT INTO PROM_TYPE_MAIN
        SELECT
                            P1.ORDER_PROM_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
                            ,P2.ORDER_ITEM_ROW_ID
                            ,P2.ORDER_ROW_ID
                            ,P2.ORDI_STAT_NAME
                            ,P2.ETL_DT
                            ,P2.CPL_DT
                            ,P2.Sales_Emp_Id
           --/* haoyf */FROM        PROM_ORDER_INFO_${LocalCode}                 P1
		   FROM        PROM_ORDER_INFO_${LocalCode}                 P1
  --/* haoyf */LEFT JOIN    EVT_ORDI_HIST_${LocalCode}         P2
  LEFT JOIN    EVT_ORDI_HIST_${LocalCode}         P2
        ON        P1.PROM_ASSET_INTEG_ID = P2.ASSET_INTEG_ID
       AND    P2.Prom_Flg = 'Y'
       and    p2.action_name = '添加'
     WHERE    P1.PROM_TYPE_NAME IN ('组合套餐','帐户级销售品','单一套餐')
;

CREATE LOCAL TEMP TABLE PROM_TYPE_OTHER ON COMMIT PRESERVE ROWS
AS(

        SELECT
        *
        FROM
        (
        SELECT 
        
        P1.ORDER_PROM_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
                            ,P2.ROOT_ORDER_ITEM_ROW_ID AS ORDER_ITEM_ROW_ID
                            ,P3.ORDER_ROW_ID
                            ,P3.ORDI_STAT_NAME
                            ,P3.CPL_DT
                            ,P3.Sales_Emp_Id
                            ,ROW_NUMBER() OVER(PARTITION BY P1.PROM_INSTANT_ROW_ID ORDER BY P2.ETL_DT  DESC    )  ROW_NUM
              --/* haoyf */FROM        PROM_ORDER_INFO_${LocalCode}            P1
			  FROM        PROM_ORDER_INFO_${LocalCode}            P1
 LEFT JOIN    STAGE_2.EVT_ORDER_PROM_ITEM_HIST_Z P2
        ON    P1.ORDER_PROM_ROW_ID = P2.ORDER_PROM_ROW_ID
         --/* haoyf */AND    P2.LATN_ID = $Latn_Id
		 AND    P2.LATN_ID = $Latn_Id
       AND    P2.PROM_ITEM_TYPE_NAME = '优惠'
 --/* haoyf */LEFT JOIN    EVT_ORDI_HIST_${LocalCode}  P3
 LEFT JOIN    EVT_ORDI_HIST_${LocalCode}  P3
        ON    P2.ROOT_ORDER_ITEM_ROW_ID = P3.ORDER_ITEM_ROW_ID
     WHERE    P1.PROM_TYPE_NAME  NOT IN ('组合套餐','帐户级销售品','单一套餐')
     ) AS T1
          WHERE ROW_NUM = 1
            AND 2=1		  
)
ORDER BY ORDER_PROM_ROW_ID,PROM_ROW_ID,PROM_TYPE_NAME
SEGMENTED BY HASH(ORDER_PROM_ROW_ID) ALL NODES KSAFE 0
;

INSERT INTO PROM_TYPE_OTHER
        
        SELECT
        *
        FROM
        (
        SELECT 
        
        P1.ORDER_PROM_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
                            ,P2.ROOT_ORDER_ITEM_ROW_ID AS ORDER_ITEM_ROW_ID
                            ,P3.ORDER_ROW_ID
                            ,P3.ORDI_STAT_NAME
                            ,P3.CPL_DT
                            ,P3.Sales_Emp_Id
                            ,ROW_NUMBER() OVER(PARTITION BY P1.PROM_INSTANT_ROW_ID ORDER BY P2.ETL_DT  DESC    )  ROW_NUM
            --/* haoyf */FROM        PROM_ORDER_INFO_${LocalCode}            P1
			FROM        PROM_ORDER_INFO_${LocalCode}           P1
 LEFT JOIN    STAGE_2.EVT_ORDER_PROM_ITEM_HIST_Z P2
        ON    P1.ORDER_PROM_ROW_ID = P2.ORDER_PROM_ROW_ID
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
	   AND    P2.LATN_ID = $Latn_Id
       AND    P2.PROM_ITEM_TYPE_NAME = '优惠'
 --/* haoyf */LEFT JOIN    EVT_ORDI_HIST_${LocalCode}  P3
 LEFT JOIN    EVT_ORDI_HIST_${LocalCode} P3
        ON    P2.ROOT_ORDER_ITEM_ROW_ID = P3.ORDER_ITEM_ROW_ID
     WHERE    P1.PROM_TYPE_NAME  NOT IN ('组合套餐','帐户级销售品','单一套餐')
     ) AS T1
          WHERE ROW_NUM = 1 
;


----【2.3】将2部分数据汇总
CREATE LOCAL TEMP TABLE PROM_HUIZONG_1 ON COMMIT PRESERVE ROWS
AS(
        SELECT         * 
          FROM 
                            (
                            SELECT
        
                                                ORDER_PROM_ROW_ID
                                                ,PROM_ROW_ID
                                                ,PROM_TYPE_NAME
                                                ,PROM_EFF_DT
                                                ,PROM_EXP_DT
                                                ,PROM_INTEG_ID
                                                ,PROM_ASSET_INTEG_ID
                                                ,STAT_NAME
                                                ,LATN_ID
                                                ,PROM_INSTANT_ROW_ID
                                                ,ORDER_ITEM_ROW_ID
                                                ,ORDER_ROW_ID
                                                ,ORDI_STAT_NAME
                                                ,CPL_DT
                                                ,Sales_Emp_Id
                                                ,HQ_OFFER_INST_ID                                    -----------V1.1
                                FROM    PROM_TYPE_MAIN
                      UNION ALL
                             SELECT
                                                ORDER_PROM_ROW_ID
                                                ,PROM_ROW_ID
                                                ,PROM_TYPE_NAME
                                                ,PROM_EFF_DT
                                                ,PROM_EXP_DT
                                                ,PROM_INTEG_ID
                                                ,PROM_ASSET_INTEG_ID
                                                ,STAT_NAME
                                                ,LATN_ID
                                                ,PROM_INSTANT_ROW_ID
                                                ,ORDER_ITEM_ROW_ID
                                                ,ORDER_ROW_ID
                                                ,ORDI_STAT_NAME
                                                ,CPL_DT
                                                ,Sales_Emp_Id
                                                ,HQ_OFFER_INST_ID                                    -----------V1.1
                                FROM    PROM_TYPE_OTHER							
) X
WHERE 2=1
GROUP BY ORDER_PROM_ROW_ID,PROM_ROW_ID,PROM_TYPE_NAME,PROM_EFF_DT,PROM_EXP_DT,PROM_INTEG_ID,PROM_ASSET_INTEG_ID,STAT_NAME,LATN_ID,PROM_INSTANT_ROW_ID,ORDER_ITEM_ROW_ID,ORDER_ROW_ID,ORDI_STAT_NAME,CPL_DT,Sales_Emp_Id,HQ_OFFER_INST_ID
)
ORDER BY ORDER_PROM_ROW_ID,PROM_ROW_ID,PROM_TYPE_NAME
SEGMENTED BY HASH(ORDER_PROM_ROW_ID) ALL NODES KSAFE 0
;


INSERT INTO PROM_HUIZONG_1
        SELECT         * 
          FROM 
                            (
                            SELECT
        
                                                ORDER_PROM_ROW_ID
                                                ,PROM_ROW_ID
                                                ,PROM_TYPE_NAME
                                                ,PROM_EFF_DT
                                                ,PROM_EXP_DT
                                                ,PROM_INTEG_ID
                                                ,PROM_ASSET_INTEG_ID
                                                ,STAT_NAME
                                                ,LATN_ID
                                                ,PROM_INSTANT_ROW_ID
                                                ,ORDER_ITEM_ROW_ID
                                                ,ORDER_ROW_ID
                                                ,ORDI_STAT_NAME
                                                ,CPL_DT
                                                ,Sales_Emp_Id
                                                ,HQ_OFFER_INST_ID                                    -----------V1.1
                                FROM    PROM_TYPE_MAIN
                      UNION ALL
                             SELECT
                                                ORDER_PROM_ROW_ID
                                                ,PROM_ROW_ID
                                                ,PROM_TYPE_NAME
                                                ,PROM_EFF_DT
                                                ,PROM_EXP_DT
                                                ,PROM_INTEG_ID
                                                ,PROM_ASSET_INTEG_ID
                                                ,STAT_NAME
                                                ,LATN_ID
                                                ,PROM_INSTANT_ROW_ID
                                                ,ORDER_ITEM_ROW_ID
                                                ,ORDER_ROW_ID
                                                ,ORDI_STAT_NAME
                                                ,CPL_DT
                                                ,Sales_Emp_Id
                                                ,HQ_OFFER_INST_ID                                    -----------V1.1
                                FROM    PROM_TYPE_OTHER    
) X
GROUP BY ORDER_PROM_ROW_ID,PROM_ROW_ID,PROM_TYPE_NAME,PROM_EFF_DT,PROM_EXP_DT,PROM_INTEG_ID,PROM_ASSET_INTEG_ID,STAT_NAME,LATN_ID,PROM_INSTANT_ROW_ID,ORDER_ITEM_ROW_ID,ORDER_ROW_ID,ORDI_STAT_NAME,CPL_DT,Sales_Emp_Id,HQ_OFFER_INST_ID
;

            
----【2.4】关联订单头信息
CREATE LOCAL TEMP TABLE PROM_HUIZONG_2 ON COMMIT PRESERVE ROWS
AS(
        SELECT
                            P1.ORDER_PROM_ROW_ID
                            ,P1.ORDER_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.ORDER_ITEM_ROW_ID
                            ,P1.ORDI_STAT_NAME
                            ,P1.CPL_DT
                            ,cast (NULL as varchar(40)) as Sales_Emp_Id                 ----发展员工标识        ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
                            ,cast (NULL as varchar(40)) as CEmployee_Row_Id             ----受理员工标识        ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
                            ,cast (NULL as varchar(30)) as Sales_Telecom_Area_ID    ----受理渠道ID            ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
                            ,cast (NULL as varchar(30)) as Sales_Dept_Id            ----发展渠道ID          ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空 
                            ,P2.CHANNEL_INNER_CODE   ----集团店中商编码
                            ,P2.CHANNEL_CODE                 ----集团渠道编码
                            ,P2.EMPLOYEE_CODE                 ----集团员工编码
                            ,P2.SALES_EMP_CODE             ----集团营销员工编码
                            ,P2.OPERATOR_CODE                 ----集团经营主体编码
                            ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
            FROM        PROM_HUIZONG_1   P1
 LEFT JOIN    STAGE_2.EVT_ORDER_HIST_Z  P2
        ON    P1.ORDER_ROW_ID = P2.ORDER_ROW_ID
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
	   AND    P2.LATN_ID = $Latn_Id
	 WHERE    2=1
----LEFT JOIN    STAGE_2.PAR_SALES_EMPLOYEE_HIST   P3
----    ON    P1.Sales_Emp_Id =  P3.Sales_Emp_Id
----   AND    P3.LATN_ID = $Latn_Id
----   AND    P3.Active_Flg = 'Y'
----   AND    P3.END_DT = '3000-12-31'
----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
---- LEFT JOIN    STAGE_2.PAR_SALES_DEPT_HIST   P4                        
----        ON    P3.SALES_DEPT_INTEG_ID = P4.SALES_DEPT_INTEG_ID
----       AND    P4.LATN_ID = $Latn_Id
----             AND    P4.END_DT = '3000-12-31'
)
ORDER BY ORDER_PROM_ROW_ID,PROM_ROW_ID,PROM_TYPE_NAME
SEGMENTED BY HASH(PROM_INSTANT_ROW_ID) ALL NODES KSAFE 0
;


INSERT INTO PROM_HUIZONG_2
        SELECT
                            P1.ORDER_PROM_ROW_ID
                            ,P1.ORDER_ROW_ID
                            ,P1.PROM_ROW_ID
                            ,P1.PROM_TYPE_NAME
                            ,P1.PROM_EFF_DT
                            ,P1.PROM_EXP_DT
                            ,P1.PROM_INTEG_ID
                            ,P1.PROM_ASSET_INTEG_ID
                            ,P1.STAT_NAME
                            ,P1.LATN_ID
                            ,P1.PROM_INSTANT_ROW_ID
                            ,P1.ORDER_ITEM_ROW_ID
                            ,P1.ORDI_STAT_NAME
                            ,P1.CPL_DT
                            ,cast (NULL as varchar(40)) as Sales_Emp_Id                 ----发展员工标识        ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
                            ,cast (NULL as varchar(40)) as CEmployee_Row_Id             ----受理员工标识        ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
                            ,cast (NULL as varchar(30)) as Sales_Telecom_Area_ID    ----受理渠道ID            ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
                            ,cast (NULL as varchar(30)) as Sales_Dept_Id            ----发展渠道ID          ----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空 
                            ,P2.CHANNEL_INNER_CODE   ----集团店中商编码
                            ,P2.CHANNEL_CODE                 ----集团渠道编码
                            ,P2.EMPLOYEE_CODE                 ----集团员工编码
                            ,P2.SALES_EMP_CODE             ----集团营销员工编码
                            ,P2.OPERATOR_CODE                 ----集团经营主体编码
                          ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
            FROM        PROM_HUIZONG_1   P1
 LEFT JOIN    STAGE_2.EVT_ORDER_HIST_Z  P2
        ON    P1.ORDER_ROW_ID = P2.ORDER_ROW_ID
       --/* haoyf */AND    P2.LATN_ID = $Latn_Id
	   AND    P2.LATN_ID = $Latn_Id
----LEFT JOIN    STAGE_2.PAR_SALES_EMPLOYEE_HIST   P3
----    ON    P1.Sales_Emp_Id =  P3.Sales_Emp_Id
----   AND    P3.LATN_ID = $Latn_Id
----   AND    P3.Active_Flg = 'Y'
----   AND    P3.END_DT = '3000-12-31'
----Modefied By L 20131104 10月31日集团规范，新增稽核规则字段放空
---- LEFT JOIN    STAGE_2.PAR_SALES_DEPT_HIST   P4
----        ON    P3.SALES_DEPT_INTEG_ID = P4.SALES_DEPT_INTEG_ID
----       AND    P4.LATN_ID = $Latn_Id
----             AND    P4.END_DT = '3000-12-31'
;             



--------【3】处理销售品状态与区域等维度汇总插入目标表
----【3.1】取当日增量关联当日增量中可取到竣工时间的数据
CREATE LOCAL TEMP TABLE PROM_HUIZONG_3 ON COMMIT PRESERVE ROWS
AS(
        SELECT
                           --/* haoyf */ '${TX_N_DATE}' AS DAY_ID
						  '${TX_N_DATE}' AS DAY_ID
                          ,CASE WHEN p1.LATN_ID = 10 THEN '83301'
                                  WHEN p1.LATN_ID = 11 THEN '83305'
                                  WHEN p1.LATN_ID = 12 THEN '83304'
                                  WHEN p1.LATN_ID = 13 THEN '83302'
                                  WHEN p1.LATN_ID = 14 THEN '83306'
                                  WHEN p1.LATN_ID = 15 THEN '83310'
                                  WHEN p1.LATN_ID = 16 THEN '83303'
                                  WHEN p1.LATN_ID = 17 THEN '83311'
                                  WHEN p1.LATN_ID = 18 THEN '83307'
                                  WHEN p1.LATN_ID = 19 THEN '83309'
                                  WHEN p1.LATN_ID = 20 THEN '83308'
                                  ELSE NULL
                            END                                                                        AS LATN_ID                                                    ----本地网标识
                            ,P1.PROM_INSTANT_ROW_ID                                                    AS OFFER_INST_ID                                        ----销售品实例ID
                            ,P1.PROM_ROW_ID                                                            AS OFFER_NBR
                            ,CASE WHEN P1.STAT_NAME = '有效' then '1000'
                                        WHEN P1.STAT_NAME IN ('无效','失效') THEN '1100'
                                        else '9999'
                                        END PO_INST_STATE_CD                 ----产品实例状态                                                                      
                            ,P2.Sales_Emp_Id                                                           AS DVLP_STAFF_ID                                        ----发展员工标识
                            ,P2.Sales_Dept_Id                                                          AS DVLP_CHNL_ID                                            ----发展渠道ID
                            ,P2.CEmployee_Row_Id                                                       AS ACCEPT_STAFF_ID                                    ----受理员工标识
                            ,P2.Sales_Telecom_Area_ID                                                  AS ACCEPT_CHNL_ID                                        ----受理渠道ID
                            ,TRIM (REPLACE ( TO_CHAR(P2.CPL_DT),'-','' )||'000000')                                    AS COMPLETE_DT                                            ----竣工时间
                            ,TRIM (REPLACE ( TO_CHAR(P1.PROM_EFF_DT),'-','' )||'000000')                            AS EFF_DATE                                                    ----生效时间
                            ,TRIM (REPLACE ( TO_CHAR(P1.PROM_EXP_DT),'-','' )||'000000')             AS EXP_DATE                                                    ----失效时间
                            ,COALESCE(P2.SALES_EMP_CODE,'-1')                             AS ACCEPT_STAFF_CD                                        ----销售员编码
                            ,COALESCE(P2.EMPLOYEE_CODE,'-1')                                AS DVLP_STAFF_CD    ----受理员编码
                            ,COALESCE(P2.CHANNEL_CODE,'-1')                                    AS SALE_OUTLETS_CD                                    ----销售点编码
                            ,COALESCE(P2.CHANNEL_INNER_CODE,'-1')              AS SALE_OUTLETS_SUB_CD              ----店中商店编码    
                            ,COALESCE(P2.OPERATOR_CODE,'-1')                                AS OPERATORS_CD                                            ----销售员所属经营主体编码    
                            ,P1.HQ_OFFER_INST_ID                                    -----------V1.1
            FROM        OFR_PROM_BSI_HIST_Z      P1
 LEFT JOIN    PROM_HUIZONG_2     P2
        ON        P1.PROM_INSTANT_ROW_ID = P2.PROM_INSTANT_ROW_ID
       AND    P2.CPL_DT IS NOT NULL
    --/* haoyf */ WHERE    P1.START_DT = '$TX_DATE'     
       WHERE    P1.START_DT = '$TX_DATE'    
         AND    2=1	   
)
SEGMENTED BY HASH (OFFER_INST_ID) ALL NODES KSAFE 0
;


INSERT INTO  PROM_HUIZONG_3
        SELECT
                           --/* haoyf */  '${TX_N_DATE}' AS DAY_ID
						   '${TX_N_DATE}' AS DAY_ID
                          ,CASE WHEN p1.LATN_ID = 10 THEN '83301'
                                  WHEN p1.LATN_ID = 11 THEN '83305'
                                  WHEN p1.LATN_ID = 12 THEN '83304'
                                  WHEN p1.LATN_ID = 13 THEN '83302'
                                  WHEN p1.LATN_ID = 14 THEN '83306'
                                  WHEN p1.LATN_ID = 15 THEN '83310'
                                  WHEN p1.LATN_ID = 16 THEN '83303'
                                  WHEN p1.LATN_ID = 17 THEN '83311'
                                  WHEN p1.LATN_ID = 18 THEN '83307'
                                  WHEN p1.LATN_ID = 19 THEN '83309'
                                  WHEN p1.LATN_ID = 20 THEN '83308'
                                  ELSE NULL
                            END                                                                                                                               AS LATN_ID                                                    ----本地网标识
                            ,P1.PROM_INSTANT_ROW_ID                                                                                          AS OFFER_INST_ID                                        ----销售品实例ID
                            ,P1.PROM_ROW_ID                                                                                                  AS OFFER_NBR
                            ,CASE WHEN P1.STAT_NAME = '有效' then '1000'
                                        WHEN P1.STAT_NAME IN ('无效','失效') THEN '1100'
                                        else '9999'
                                        END PO_INST_STATE_CD                 ----产品实例状态                                                                      
                            ,P2.Sales_Emp_Id                                                                                                         AS DVLP_STAFF_ID                                        ----发展员工标识
                            ,P2.Sales_Dept_Id                                                                                                AS DVLP_CHNL_ID                                            ----发展渠道ID
                            ,P2.CEmployee_Row_Id                                                                                                    AS ACCEPT_STAFF_ID                                    ----受理员工标识
                            ,P2.Sales_Telecom_Area_ID                                                                                    AS ACCEPT_CHNL_ID                                        ----受理渠道ID
                            ,TRIM (REPLACE ( TO_CHAR(P2.CPL_DT),'-','' )||'000000')                                    AS COMPLETE_DT                                            ----竣工时间
                            ,TRIM (REPLACE ( TO_CHAR(P1.PROM_EFF_DT),'-','' )||'000000')                            AS EFF_DATE                                                    ----生效时间
                            ,TRIM (REPLACE ( TO_CHAR(P1.PROM_EXP_DT),'-','' )||'000000')             AS EXP_DATE                                                    ----失效时间
                            ,COALESCE(P2.SALES_EMP_CODE,'-1')                             AS ACCEPT_STAFF_CD  ----受理员编码
                            ,COALESCE(P2.EMPLOYEE_CODE,'-1')                                AS DVLP_STAFF_CD                                     ----销售员编码
                            ,COALESCE(P2.CHANNEL_CODE,'-1')                                    AS SALE_OUTLETS_CD                                    ----销售点编码
                            ,COALESCE(P2.CHANNEL_INNER_CODE,'-1')              AS SALE_OUTLETS_SUB_CD              ----店中商店编码    
                            ,COALESCE(P2.OPERATOR_CODE,'-1')                                AS OPERATORS_CD                                            ----销售员所属经营主体编码
                        ,P1.HQ_OFFER_INST_ID                                    -----------V1.1    
            FROM        OFR_PROM_BSI_HIST_Z      P1
 LEFT JOIN    PROM_HUIZONG_2     P2
        ON        P1.PROM_INSTANT_ROW_ID = P2.PROM_INSTANT_ROW_ID
       AND    P2.CPL_DT IS NOT NULL
   --/* haoyf */  WHERE    P1.START_DT = '$TX_DATE'    
     WHERE    P1.START_DT = '$TX_DATE'  
;


----【4】终端应补金额
CREATE LOCAL TEMP TABLE PROM_HUIZONG_4 ON COMMIT PRESERVE ROWS
AS(
        SELECT
                            P1.DAY_ID        
                            ,P1.LATN_ID
                            ,P1.OFFER_INST_ID
                            ,P1.OFFER_NBR
                            ,P1.PO_INST_STATE_CD
                            ,P1.DVLP_STAFF_ID
                            ,P1.DVLP_CHNL_ID
                            ,P1.ACCEPT_STAFF_ID
                            ,P1.ACCEPT_CHNL_ID
                            ,P1.COMPLETE_DT
                            ,P1.EFF_DATE
                            ,P1.EXP_DATE
                            ,P1.DVLP_STAFF_CD
                            ,P1.ACCEPT_STAFF_CD
                            ,P1.SALE_OUTLETS_CD
                            ,P1.SALE_OUTLETS_SUB_CD
                            ,P1.OPERATORS_CD
                            ,P1.HQ_OFFER_INST_ID  AS  EXT_PROD_OFFER_INST_ID                                  -----------V1.1    
                            ----,COALESCE(P2.COUPON_AMT,0) AS TRMNL_SHOULD_FEE
            FROM        PROM_HUIZONG_3 P1
			WHERE       2=1
 ----LEFT join        ITFJYFX_2DAPD_FIN_ALWANC_Z    P2
 ----                ON        P1.OFFER_INST_ID = P2.ALLWNC_PO_INST_ID
 ----             and    P1.LATN_ID = P2.LATN_ID
 ----             and    P2.ALLWNC_PO_INST_ID <> '-1'
)
SEGMENTED BY HASH (OFFER_INST_ID) ALL NODES KSAFE 0
;

INSERT INTO PROM_HUIZONG_4
        SELECT
                            P1.DAY_ID        
                            ,P1.LATN_ID
                            ,P1.OFFER_INST_ID
                            ,P1.OFFER_NBR
                            ,P1.PO_INST_STATE_CD
                            ,P1.DVLP_STAFF_ID
                            ,P1.DVLP_CHNL_ID
                            ,P1.ACCEPT_STAFF_ID
                            ,P1.ACCEPT_CHNL_ID
                            ,P1.COMPLETE_DT
                            ,P1.EFF_DATE
                            ,P1.EXP_DATE
                            ,P1.DVLP_STAFF_CD
                            ,P1.ACCEPT_STAFF_CD
                            ,P1.SALE_OUTLETS_CD
                            ,P1.SALE_OUTLETS_SUB_CD
                            ,P1.OPERATORS_CD
                            ,P1.HQ_OFFER_INST_ID  AS  EXT_PROD_OFFER_INST_ID                                     -----------V1.1    
                            ----,COALESCE(P2.COUPON_AMT,0) AS TRMNL_SHOULD_FEE
            FROM        PROM_HUIZONG_3 P1
 ----LEFT join        ITFJYFX_2DAPD_FIN_ALWANC_Z    P2
 ----                ON        P1.OFFER_INST_ID = P2.ALLWNC_PO_INST_ID
 ----             and    P1.LATN_ID = P2.LATN_ID
 ----             and    P2.ALLWNC_PO_INST_ID <> '-1'
;


                                                      
                                                      
----【3.2】数据插入目标表    
--/* haoyf */DELETE FROM ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
--/* haoyf */WHERE  DAY_ID =  '${TX_N_DATE}' or DAY_ID = '$Begfore_Day2_Yyyy_Mm_N_Dd'
DELETE FROM ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
WHERE  DAY_ID =  '${TX_N_DATE}' or DAY_ID = '$Begfore_Day2_Yyyy_Mm_N_Dd'
;

--/* haoyf*/INSERT INTO ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
INSERT INTO ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
(
DAY_ID                                                
,LATN_ID                      
,OFFER_INST_ID                
,OFFER_NBR                    
,PO_INST_STATE_CD             
,DVLP_STAFF_ID                
,DVLP_CHNL_ID                 
,ACCEPT_STAFF_ID              
,ACCEPT_CHNL_ID               
,COMPLETE_DT                  
,EFF_DATE                     
,EXP_DATE                     
,DVLP_STAFF_CD                
,ACCEPT_STAFF_CD              
,SALE_OUTLETS_CD              
,SALE_OUTLETS_SUB_CD          
,OPERATORS_CD      
,TRMNL_SHOULD_FEE
,EXT_PROD_OFFER_INST_ID                                     -----------V1.1             
 )
        SELECT 
                            --/* haoyf*/'${TX_N_DATE}'
							'${TX_N_DATE}'
                            ,LATN_ID                      
                            ,OFFER_INST_ID                
                            ,OFFER_NBR                    
                            ,PO_INST_STATE_CD             
                            ,DVLP_STAFF_ID                
                            ,DVLP_CHNL_ID                 
                            ,ACCEPT_STAFF_ID              
                            ,ACCEPT_CHNL_ID               
                            ,COMPLETE_DT                  
                            ,EFF_DATE                     
                            ,EXP_DATE                     
                            ,DVLP_STAFF_CD                
                            ,ACCEPT_STAFF_CD              
                            ,SALE_OUTLETS_CD              
                            ,SALE_OUTLETS_SUB_CD          
                            ,OPERATORS_CD  
                            ,CAST ( 0 AS DECIMAL (16,2)) AS TRMNL_SHOULD_FEE  
                            ,EXT_PROD_OFFER_INST_ID                                     -----------V1.1    
          --/* haoyf*/  from        ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
		    from        ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
         --/* haoyf*/WHERE    DAY_ID = '${Begfore_Day1_Yyyy_Mm_N_Dd}'
		 WHERE    DAY_ID = '${Begfore_Day1_Yyyy_Mm_N_Dd}'
;

--/* haoyf*/MERGE INTO ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_ AS P1
MERGE INTO ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4 AS P1
USING 
(
SELECT                            
DAY_ID                                                
,LATN_ID                      
,OFFER_INST_ID                
,OFFER_NBR                    
,PO_INST_STATE_CD             
,DVLP_STAFF_ID                
,DVLP_CHNL_ID                 
,ACCEPT_STAFF_ID              
,ACCEPT_CHNL_ID               
,COMPLETE_DT                  
,EFF_DATE                     
,EXP_DATE                     
,DVLP_STAFF_CD                
,ACCEPT_STAFF_CD              
,SALE_OUTLETS_CD              
,SALE_OUTLETS_SUB_CD          
,OPERATORS_CD          
,CAST ( 0 AS DECIMAL (16,2)) AS TRMNL_SHOULD_FEE
,EXT_PROD_OFFER_INST_ID                                     -----------V1.1    
FROM PROM_HUIZONG_4
)AS P2
ON P1.OFFER_INST_ID=P2.OFFER_INST_ID
--/* haoyf*/AND P1.DAY_ID = '${TX_N_DATE}'
AND P1.DAY_ID = '${TX_N_DATE}'
WHEN MATCHED THEN UPDATE
SET 
                            EFF_DATE = p2.EFF_DATE
                            ,EXP_DATE = p2.EXP_DATE
                            ,PO_INST_STATE_CD = p2.PO_INST_STATE_CD
WHEN NOT MATCHED THEN
INSERT VALUES 
(
                            P2.DAY_ID
                            ,P2.LATN_ID                      
                            ,P2.OFFER_INST_ID                
                            ,P2.OFFER_NBR                    
                            ,P2.PO_INST_STATE_CD             
                            ,P2.DVLP_STAFF_ID                
                            ,P2.DVLP_CHNL_ID                 
                            ,P2.ACCEPT_STAFF_ID              
                            ,P2.ACCEPT_CHNL_ID               
                            ,P2.COMPLETE_DT                  
                            ,P2.EFF_DATE                     
                            ,P2.EXP_DATE                     
                            ,P2.DVLP_STAFF_CD                
                            ,P2.ACCEPT_STAFF_CD              
                            ,P2.SALE_OUTLETS_CD              
                            ,P2.SALE_OUTLETS_SUB_CD          
                            ,P2.OPERATORS_CD 
                            ,p2.TRMNL_SHOULD_FEE
                            ,p2.EXT_PROD_OFFER_INST_ID                                     -----------V1.1       
)
;

--/* haoyf*/UPDATE ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
UPDATE ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
SET EXP_DATE = EFF_DATE
WHERE EFF_DATE > EXP_DATE
--/* haoyf*/and DAY_ID = '${TX_N_DATE}'
and DAY_ID = '${TX_N_DATE}'
;



--/* haoyf*/UPDATE ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
UPDATE ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
SET EXP_DATE = COMPLETE_DT
WHERE COMPLETE_DT > EXP_DATE
--/* haoyf*/and DAY_ID = '${TX_N_DATE}'
and DAY_ID = '${TX_N_DATE}'
;

--/* haoyf*/DELETE FROM ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
DELETE FROM ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
--/* haoyf*/WHERE EXP_DATE <= '$CUR_YEAR'||'0101000000'
WHERE EXP_DATE <= '$CUR_YEAR'||'0101000000'
;

MERGE INTO 
--/* haoyf*/ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4 AS P1 
ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4 AS P1 
USING 
(
SELECT 
ALLWNC_PO_INST_ID,COUPON_AMT
FROM 
ITFJYFX_2.DAPD_FIN_ALWANC_Z 
--/* haoyf*/WHERE LATN_ID = '${Jt_Latn_Id}'
WHERE LATN_ID = '${Jt_Latn_Id}'
and   ALLWNC_PO_INST_ID <> '-1'
)
AS P2
                ON        P1.OFFER_INST_ID = P2.ALLWNC_PO_INST_ID  
             --/* haoyf*/AND    P1.day_id = '${TX_N_DATE}'
			 AND    P1.day_id = '${TX_N_DATE}'
WHEN MATCHED THEN UPDATE
SET 
TRMNL_SHOULD_FEE = COALESCE(P2.COUPON_AMT,0)
--ELSE     IGNORE
;

--/* haoyf*/UPDATE ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
UPDATE ITFJYFX_2.DAPD_PRD_PO_INST_${LocalCode}_4
SET TRMNL_SHOULD_FEE = 0
WHERE TRMNL_SHOULD_FEE < 0   
--/* haoyf*/and day_id = '${TX_N_DATE}'
and day_id = '${TX_N_DATE}'
;

INPUT
;


sub run_bteq_command
{
	my ($sql1) = @_;
	my $rc = open(BTEQ, "| /opt/vertica/bin/vsql -U dbadmin -w dbadmin -e");
	unless ($rc)
	{
		print "Could not invoke BTEQ command\n";
		return -1;
	}
  my $QRY_BAND = substr($Control_File,0,length($Control_File)-4);
# ------ Below are BTEQ scripts ------
	print BTEQ <<ENDOFINPUT;

\\set AUTOCOMMIT on
\\timing
\\set ON_ERROR_STOP on

$sql1

\\q

ENDOFINPUT
	close(BTEQ);
	my $RET_CODE = $? >> 8;

	if ( $RET_CODE == 12 ) {
		return 1;
	}
	else {
		return 0;
	}
}                   #End of BTEQ function


##------------ main function ------------
sub main
{
	###在主函数中直接调用下面的程序，不用修改
   my $ret ;
   print "SQL Begin time : ".&getnow()."\n";

#   $ret=ZJDW_DB2::run_db2_command($sql);
   $ret=run_bteq_command($sql);
   print "SQL End time : ".&getnow()."\n";
   print "rc=$ret\n";
   return $ret;

 	
}


open(STDERR, ">&STDOUT");
exit(main());
    

###--------------------------------------------------获取当前时间
sub getnow
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time());
   my $current="";
    
    $year=sprintf("%02d",$year+1900);
    $mon =sprintf("%02d",$mon+1);
    $mday=sprintf("%02d",$mday);
    $hour=sprintf("%02d",$hour);
    $min =sprintf("%02d",$min);
    $sec =sprintf("%02d",$sec);
    $current="${year}-${mon}-${mday} ${hour}:${min}:${sec}";
    return $current;
}
