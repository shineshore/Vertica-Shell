#!/user/bin/perl                                                                                             
use strict;     # Declare usINg Perl strict syntax

#####################################################################################################
# ------------ Variable SectiON :DATABSE DEF&INI------------     
my $AUTO_HOME = $ENV{"AUTO_HOME"};
#my $TARGETDB =  "ZJBIC_1";    
my $TARGETDB =  $ENV{"AUTO_MDATADB"};   
my $WORKDB   =  $ENV{"AUTO_MWORKDB"};
my $SOURCEDB =  $ENV{"AUTO_MVIEWDB"};            


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

my $SCRIPT = "此perl脚本名称";#非必要参数
# ------------ BTEQ function ------------
sub run_bteq_command
{
	my $rc = open(BTEQ, "| /opt/vertica/bin/vsql -U dbadmin -w dbadmin -e");
	unless ($rc)
	{
		print "Could not invoke BTEQ command\n";
		return -1;
	}
  my $QRY_BAND = substr($CONTROL_FILE,0,length($CONTROL_FILE)-4);
# ------ Below are BTEQ scripts ------
	print BTEQ <<ENDOFINPUT;

\\set AUTOCOMMIT on
\\timing
\\set ON_ERROR_STOP on

--------------资产主要信息
CREATE LOCAL TEMP table OFR_MAIN_ASSET_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT     P1.Asset_Row_Id 
           ,TO_DATE('$TX_DATE' ,  'YYYYMMDD') AS Start_Dt
           ,P1.Start_Dt AS Real_Start_Dt
           ,P1.Serv_Start_Dt
           ,P1.Serv_End_Dt
           ,P12.Std_Prd_Lvl4_Name
           ,P12.Std_Prd_Lvl3_Id
           ,P1.STD_PRD_LVL4_ID
           ,(CASE WHEN P1.Stat_Name IN ('现行','不活动','已暂停')
                   THEN P1.Stat_Name
                   ELSE 'err'
               END) AS  Stat_Name        
            ,(CASE WHEN P1.ODS_Stat_Name IN ('F0K','F0P') THEN '双向'       ---------V2.1
									 WHEN P1.ODS_Stat_Name ='F0M' THEN '单向'
                   else 'err'
                    END  ) AS Owe_Suspend_Name             
            ,(CASE WHEN P1.Pre_Active_Status='预开户1' THEN '预开户'
                  WHEN P1.Pre_Active_Status='正常' THEN '正常开户'
                  ELSE P1.Pre_Active_Status
              END) AS Pre_Active_Status  --预开户状态    
            ,P1.LATN_ID        
            ,P1.STAT_CHANGE_DT     ----V2.0
      FROM  BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode}   P1
INNER JOIN  DMN_1.CAG_COM_STD_PRD_LVL4                  P12
        ON  P1.Std_Prd_Lvl4_Id=P12.Std_Prd_Lvl4_Id
     WHERE  P1.Start_Dt<=TO_DATE('$TX_DATE' ,  'YYYYMMDD')
	     AND  P1.End_Dt  >TO_DATE('$TX_DATE' ,  'YYYYMMDD')
	     AND  P12.PRD_ID IN('40','60','50','10','70')   --CDMA\ITV\宽带\普通电话
	     --AND  (P1.stat_name <>'不活动'  OR  (stat_name ='不活动' and start_dt>=DATE('$TX_DATE') -5 DAYS) )  
)
ORDER BY Asset_Row_Id, P1.STD_PRD_LVL4_ID
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      



-------------测试用户标记
CREATE LOCAL TEMP table MAIN_FREE_CORP_PAY_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
select   Asset_Row_Id
            ,MAX(CASE WHEN P.Cdsc_Row_Id='1-19849-1' THEN 1 ELSE 0 END) AS Test_Flg
      FROM  BSSDATA_1.OFR_ASSET_CDSC_HIST_${LocalCode}       P  
     WHERE  P.Stat_Name ='使用中'
       AND  P.Start_Dt<=TO_DATE('$TX_DATE' ,  'YYYYMMDD')
       AND  P.End_Dt>TO_DATE('$TX_DATE' ,  'YYYYMMDD')
       AND  P.Asset_Row_Id IN (SELECT Asset_Row_Id FROM OFR_MAIN_ASSET_${LocalCode})
      GROUP BY Asset_Row_Id 
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      


---------------停复机流水信息
CREATE LOCAL TEMP table STOP_ASSET_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
    SELECT  Asset_Row_Id
        		,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P') THEN 1 else 0 END) AS Twoway_Stop_Flg   ----F0K（双停）    ----V2.1    
    		    ,MAX(CASE WHEN Call_Limit_Stat='F0M' THEN 1 else 0 END) AS Oneway_Stop_Flg   ----F0M（单停）  
            ,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P')                                         ----F0K（双停）    ----V2.1 
                      THEN TO_DATE('19990101' ,  'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Deal_Time)  ,  'YYYYMMDD')
                  END ) AS Oneway_Stop_Dt
            ,MAX(CASE WHEN Call_Limit_Stat='F0M'                                         ----F0M（单停）
                      THEN TO_DATE('19990101' ,  'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Deal_Time) ,  'YYYYMMDD')
                  END ) AS Twoway_Stop_Dt
      FROM  BSSDATA_1.OFR_ASSET_CALL_LIMIT_LOG_${LocalCode}
     WHERE  TO_DATE(TO_CHAR(Deal_Time)  ,  'YYYYMMDD')<=TO_DATE('${TX_DATE}' ,  'YYYYMMDD')
       AND  Call_Limit_Stat IN('F0M','F0K','F0P')                                        ----V4.5 
     GROUP BY Asset_Row_Id
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      




CREATE LOCAL TEMP table STOP_ASSET_1_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
    SELECT  Asset_Row_Id
        		,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P') THEN 1 else 0 END) AS Twoway_Stop_Flg1      ----V2.1 
    		    ,MAX(CASE WHEN Call_Limit_Stat='F0M' THEN 1 else 0 END) AS Oneway_Stop_Flg1                
            ,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P')                                             ----V2.1 
                      THEN TO_DATE('19990101' ,  'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Stat_Upd_Time) ,  'YYYYMMDD')
                  END ) AS Oneway_Stop_Dt1
            ,MAX(CASE WHEN Call_Limit_Stat='F0M' 
                      THEN TO_DATE('19990101' ,  'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Stat_Upd_Time) ,  'YYYYMMDD')
                  END ) AS Twoway_Stop_Dt1
      FROM  BSSDATA_1.OFR_ASSET_CALL_LIMIT_LOG_${LocalCode}
     WHERE  TO_DATE(TO_CHAR(Stat_Upd_Time) ,  'YYYYMMDD')<=TO_DATE('${TX_DATE}' ,  'YYYYMMDD')
       AND  Deal_Time IS NULL
       AND  Call_Limit_Stat IN('F0M','F0K','F0P')
     GROUP BY Asset_Row_Id
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      



CREATE LOCAL TEMP table OWE_92_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT  p.Asset_Integ_Id            ----------alter by chenk 20130121,更改关联字段，ASSET_INTEG_ID-->ASSET_ROW_ID
            ,p1.ASSET_ROW_ID
            ,SUM(Amt)   Amt
     FROM  BSSDATA_1.FIN_PG_OWE_STMT_ITEM_D_${LocalCode} p
LEFT JOIN  BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode} P1
       ON  P.Asset_Integ_Id=P1.Asset_Integ_Id
      AND  P1.END_DT=DATE'3000-12-31'
    WHERE  Stmt_Dt <= TO_DATE('$TX_DATE' ,  'YYYYMMDD')- 91 
      AND  FLG='1'
  GROUP BY  p.Asset_Integ_Id
            ,p1.ASSET_ROW_ID
)
ORDER BY p.Asset_Integ_Id, Asset_Row_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      



CREATE LOCAL TEMP table MAIN_PRE_1_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT       P1.Asset_Row_Id
              ,P1.Start_Dt
              ,P1.Stat_Name     
              ,P1.Serv_Start_Dt
              ,P1.Serv_End_Dt       
              ,P1.Owe_Suspend_Name
              ,P1.Pre_Active_Status
              ,P1.Std_Prd_Lvl4_Id
              ,P1.Std_Prd_Lvl4_Name
              ,P1.Std_Prd_Lvl3_Id 
              ,COALESCE(P18.Test_Flg,0) AS Test_Flg                   
             ,(CASE WHEN P1.Stat_Name='已暂停' AND P1.Owe_Suspend_Name='单向' 
                          THEN (CASE WHEN P34.Oneway_Stop_Flg = 1 AND P34.Oneway_Stop_Dt IS NOT NULL
                                    THEN P34.Oneway_Stop_Dt
                                    WHEN P33.Oneway_Stop_Dt1 IS NOT NULL AND P34.Oneway_Stop_Dt IS  NULL
                                    THEN P33.Oneway_Stop_Dt1
                                    ELSE P1.Real_Start_Dt
                                END)
                          ELSE NULL
                      END) AS Oneway_Stop_Dt                  ----V1.3
              ,(CASE WHEN P1.Stat_Name='已暂停' AND P1.Owe_Suspend_Name='双向' 
                          THEN (CASE WHEN P34.Twoway_Stop_Flg = 1 AND P34.Twoway_Stop_Dt IS NOT NULL
                                     THEN P34.Twoway_Stop_Dt
                                     WHEN P33.Twoway_Stop_Dt1 IS NOT NULL  AND P34.Twoway_Stop_Dt IS NULL
                                     THEN P33.Twoway_Stop_Dt1
                                     ELSE P1.Real_Start_Dt
                                END)
                          ELSE NULL
                      END) AS Twoway_Stop_Dt                    ----V1.3
             ,(CASE WHEN P34.Twoway_Stop_Flg = 1 AND P34.Twoway_Stop_Dt IS NOT NULL
                                     THEN P34.Twoway_Stop_Dt
                                     WHEN P33.Twoway_Stop_Dt1 IS NOT NULL  AND P34.Twoway_Stop_Dt IS NULL
                                     THEN P33.Twoway_Stop_Dt1
                                     ELSE NULL
                                END) AS Twoway_Stop_Dt1       ----V1.3
              ,(CASE WHEN P2.Asset_Row_Id IS NOT NULL AND P1.Std_Prd_Lvl4_Id IN (11010501) THEN 1 ELSE 0 END) AS Own_Flg
              ,P1.LATN_ID
        FROM  OFR_MAIN_ASSET_${LocalCode}                            P1
   LEFT JOIN  STOP_ASSET_${LocalCode}                           P34
          ON  P1.Asset_Row_Id=P34.Asset_Row_Id
   LEFT JOIN  STOP_ASSET_1_${LocalCode}                         P33
          ON  P1.Asset_Row_Id=P33.Asset_Row_Id       
   LEFT JOIN  MAIN_FREE_CORP_PAY_${LocalCode}                   P18
          ON  P1.Asset_Row_Id=P18.Asset_Row_Id
   LEFT JOIN  OWE_92_${LocalCode}              P2
          ON  P1.Asset_Row_Id = P2.Asset_Row_Id
         AND  P2.Amt > 0
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      




CREATE LOCAL TEMP table MAIN_PRE_2_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT  P1.*
                 ,(CASE WHEN P1.Std_Prd_Lvl4_Id IN (11010200,11010201,11010202,11010307) AND P20.Stat_Name IS NOT NULL AND P20.Stat_Name IN ('IPAS激活','CRM预占')     THEN '正常开户'
                        WHEN P1.Std_Prd_Lvl4_Id IN (11010200,11010201,11010202,11010307) AND P20.Stat_Name IS NOT NULL AND P20.Stat_Name NOT IN ('IPAS激活','CRM预占') THEN '预开户'
                        ELSE P1.Pre_Active_Status
                   END) AS Pre_Active_Status1
           FROM    MAIN_PRE_1_${LocalCode}                                 P1     
      LEFT JOIN   (SELECT  Asset_Row_Id
                          ,MIN(Stat_Name) AS Stat_Name
                    FROM  BSSDATA_1.OFR_ASSET_INST_STAT_HIST_${LocalCode}
                   WHERE  Asset_Row_Id IS NOT NULL
                     AND  Asset_Row_Id <> '-1'
                     AND  Start_Dt<=TO_DATE('$TX_DATE' ,  'YYYYMMDD')
                     AND  End_Dt>TO_DATE('$TX_DATE' ,  'YYYYMMDD')
                 GROUP BY Asset_Row_Id)                                              P20
             ON  	 P1.Asset_Row_Id=P20.Asset_Row_Id   
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      



-------------速率
CREATE LOCAL TEMP table TMP_SPEED_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT ASSET_ROW_ID,VAL FROM (							  
SELECT
						ROW_NUMBER() OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY CASE WHEN (P2.VAL IS NOT NULL AND P2.VAL <> '-1') THEN P2.Etl_Dt ELSE P1.Etl_Dt END DESC) AS Q_RANK
						,P1.ASSET_ROW_ID
						,CASE WHEN (P2.VAL IS NOT NULL AND P2.VAL <> '-1') THEN P2.VAL
									ELSE P1.VAL
						END VAL
FROM 				BSSDATA_1.OFR_ASSET_EXI_HIST_${LocalCode} P1
LEFT JOIN   BSSDATA_1.OFR_ASSET_EXI_HIST_${LocalCode} P2
ON					P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
AND         P2.Start_Dt<=TO_DATE('$TX_DATE' ,  'YYYYMMDD')
AND         P2.End_Dt>TO_DATE('$TX_DATE' ,  'YYYYMMDD') 
AND         P2.VAL_TYPE_NAME = '使用速率'
WHERE				P1.Start_Dt<=TO_DATE('$TX_DATE' ,  'YYYYMMDD')
AND         P1.End_Dt>TO_DATE('$TX_DATE' ,  'YYYYMMDD') 
AND 				P1.VAL_TYPE_NAME = '下载速率'
) T WHERE Q_RANK   = 1
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      


-------------插入ITV速率        ------------------V2.2
INSERT INTO TMP_SPEED_${LocalCode}
SELECT ASSET_ROW_ID,VAL FROM (							  
SELECT
						ROW_NUMBER() OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY CASE WHEN P1.VAL_TYPE_NAME = '速率' THEN 2
                                                                         WHEN P1.VAL_TYPE_NAME = '端口速率' THEN 1
                                                                    else 0 END DESC) AS Q_RANK
						,P1.ASSET_ROW_ID
						,CASE WHEN P1.VAL_TYPE_NAME = '速率' THEN P1.VAL
                  WHEN P1.VAL_TYPE_NAME = '端口速率' THEN P1.VAL
                ELSE 'ERR'
            END VAL
FROM 				BSSDATA_1.OFR_ASSET_EXI_HIST_${LocalCode} P1
INNER JOIN  MAIN_PRE_2_${LocalCode} P2
        ON  P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
     WHERE	P1.Start_Dt<=TO_DATE('$TX_DATE' ,  'YYYYMMDD')
       AND  P1.End_Dt>TO_DATE('$TX_DATE' ,  'YYYYMMDD') 
       AND 	P1.VAL_TYPE_NAME IN ('速率','端口速率')
       AND  P2.STD_PRD_LVL4_ID IN(14030501,14030500)
) T WHERE Q_RANK     = 1       
;


-------------V2.0集团统一计费口径

CREATE LOCAL TEMP TABLE OWE_STMT_1_${LocalCode}  ON COMMIT PRESERVE ROWS
AS 
(select   					P1.ASSET_ROW_ID--V2.7 改用Asset_row_id关联
                    ,P1.Owe_Stmt_Dt
                    ,P1.Stmt_Dt
                    ,SUM(P1.AMT) AS OWE_AMT      ----按照资产集成编号进行
             FROM   BSSDATA_1.FIN_PG_OWE_STMT_ITEM_D_${LocalCode}   P1
       INNER JOIN		OFR_MAIN_ASSET_${LocalCode} P2
       				 ON		P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
       				AND   P2.STAT_NAME <> '不活动'
            WHERE   P1.FLG IN ('1','2')
              AND   P1.Bad_Debt_Flg = 'N'
   GROUP BY 1,2,3
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0;



CREATE LOCAL TEMP table OWE_STMT_2_${LocalCode} ON COMMIT PRESERVE ROWS
AS 
(select             ASSET_ROW_ID
                    ,MAX(CAST(EXTRACT(YEAR FROM Owe_Stmt_Dt) - EXTRACT(YEAR FROM Stmt_Dt) AS INTEGER)*12+CAST(EXTRACT(MONTH FROM Owe_Stmt_Dt)-(EXTRACT(MONTH FROM Stmt_Dt)+1) AS INTEGER)) NBR
            FROM    OWE_STMT_1_${LocalCode} 
           WHERE    OWE_AMT > 0
           GROUP BY 1
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0;



CREATE LOCAL TEMP TABLE NEW_BIL_FLG_${LocalCode}_2 ON COMMIT PRESERVE ROWS
AS
(
		SELECT
							P1.ASSET_ROW_ID
							,CASE WHEN P1.STAT_NAME = '不活动' THEN 0
										WHEN P1.Pre_Active_Status ='预开户' THEN 0
										WHEN P1.STD_PRD_LVL4_ID IN ('11020413','11020419') AND P1.STAT_NAME = '已暂停' AND P1.STAT_CHANGE_DT < TO_DATE ('$TX_DATE' ,  'YYYYMMDD')-31 THEN 0
										WHEN P1.STD_PRD_LVL4_ID NOT IN ('11020413','11020419') AND P2.ASSET_ROW_ID IS NOT NULL AND P2.NBR >= 3 THEN 0
										ELSE 1
							END AS BIL_FLG
			FROM		OFR_MAIN_ASSET_${LocalCode} P1
 LEFT JOIN		OWE_STMT_2_${LocalCode} P2
 				ON		P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;


----V4.0取出限制外网资产数据
CREATE LOCAL TEMP TABLE NEW_BIL_FLG_${LocalCode} ON COMMIT PRESERVE ROWS
AS
(
SELECT ASSET_ROW_ID,BIL_FLG FROM (								  
		SELECT
							ROW_NUMBER () OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY BIL_FLG) AS Q_RANK
							,P1.ASSET_ROW_ID					   
							,CASE WHEN P2.ASSET_ROW_ID IS NOT NULL THEN 0 ELSE P1.BIL_FLG END BIL_FLG
			FROM		NEW_BIL_FLG_${LocalCode}_2 P1
 LEFT JOIN    BSSDATA_1.OFR_CHILD_ASSET_HIST_${LocalCode} P2
        ON    P1.ASSET_ROW_ID = P2.ROOT_ASSET_ROW_ID
       AND    P2.START_DT <= TO_DATE('${TX_DATE}' ,  'YYYYMMDD')
       AND    P2.End_Dt > TO_DATE('${TX_DATE}' ,  'YYYYMMDD')
       AND    P2.CPRD_ROW_ID = '1-HKOIMR0'
       AND    P2.STAT_NAME <> '不活动'
) T WHERE Q_RANK = 1
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;





CREATE LOCAL TEMP table MAIN_PRE_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT            P1.Asset_Row_Id
                 ,P1.Start_Dt AS Date_Cd
                 ,P1.Std_Prd_Lvl4_Name
                 ,P1.Stat_Name
                 ,P1.Serv_End_Dt
                 ,P1.Std_Prd_Lvl4_Id
                 ,P1.Owe_Suspend_Name
                 ,Pre_Active_Status1
                 ,P1.Oneway_Stop_Dt
                 ,P1.Twoway_Stop_Dt
                 ,(CASE WHEN P1.Std_Prd_Lvl4_Id IN (11010502,11020601,11010200,11010201,11010202,11010307)    ---V1.4 WUJJ 增加上网卡预付费的判断
                        THEN ( CASE WHEN P1.Stat_Name = '不活动' THEN 0          ---ADD BY ZN20100407
                   WHEN (Pre_Active_Status1 NOT IN ('预开户','移动即买即通') OR Pre_Active_Status1 IS NULL)     ---ADD BY CHENK 20110919 天翼主副卡，副卡未激活状态标识                        
                                         AND P1.Stat_Name <> '不活动'
                                         AND P1.Test_Flg = 0
                                         AND P1.Own_Flg = 0         
                                         AND NOT (COALESCE(P1.Owe_Suspend_Name,'-1') IN ('双向') 
                                                  AND P1.TwoWay_stop_Dt IS NOT NULL
                                                  AND P1.TwoWay_stop_Dt <= TO_DATE('$TX_DATE' ,  'YYYYMMDD')-62  
                                                  )
                                                 THEN 1                             
                                    ELSE 0
                                    END )
                         WHEN P1.Std_Prd_Lvl4_Id IN (11010501) 
                         THEN ( CASE WHEN P1.Stat_Name = '不活动' THEN 0          ---ADD BY ZN20100407
                                 
                                   WHEN (Pre_Active_Status1 NOT IN ('预开户','移动即买即通') OR Pre_Active_Status1 IS NULL)---ADD BY CHENK 20110919 天翼主副卡，副卡未激活状态标识
                                         AND P1.Stat_Name <> '不活动'
                                         AND P1.Test_Flg = 0
                                         AND NOT (P1.Own_Flg = 1 
                                                  AND COALESCE(P1.Owe_Suspend_Name,'-1') IN ('双向') 
                                                  AND P1.TwoWay_stop_Dt IS NOT NULL
                                                  AND P1.TwoWay_stop_Dt <= TO_DATE('$TX_DATE' ,  'YYYYMMDD')-62  
                                                  )
                                                 THEN 1                             
                                    ELSE 0
                                    END )
                             ELSE NULL
                   END) AS On_Serv_Flg      ----V1.3
                 ----,(CASE  WHEN  P16.Asset_Row_Id  IS not NULL  THEN
                 ----              (CASE WHEN P1.STAT_NAME = '不活动' THEN 0   -----V2.1 ADD BY LUT 增加拆机条件限制
                 ----                    WHEN P16.Bil_Flg='Y' THEN 1
                 ----                    WHEN P16.Bil_Flg='N' THEN 0 
                 ----               ELSE 0
                 ----               END)
                 ----        WHEN  P16.Asset_Row_Id  IS  NULL     THEN
                 ----             -- (CASE  WHEN (Pre_Active_Status1 <> '预开户' OR Pre_Active_Status1 IS NULL) 
                 ----              (CASE  WHEN (Pre_Active_Status1 not in ('预开户','移动即买即通') OR Pre_Active_Status1 IS NULL) ---ADD BY CHENK 20110914 天翼主副卡，副卡未激活状态标识
                 ----                          AND P1.Stat_Name <> '不活动'
                 ----                     THEN  1                                
                 ----               ELSE 0
                 ----               END)
                 ----       ELSE 0             
                 ----  END )  AS Bil_Flg		----1.3   ----V2.0注释
                 ,CASE WHEN P2.ASSET_ROW_ID IS NOT NULL THEN P2.BIL_FLG
                 ELSE 0
                 END AS BIL_FLG  ----v2.0 新计费口径
                   ,(CASE WHEN P1.Stat_Name = '不活动' THEN 0          ---ADD BY ZN20100407
                        WHEN Pre_Active_Status1 = '预开户' THEN 0
                        WHEN Pre_Active_Status1 = '移动即买即通' THEN 0   ---ADD BY CHENK 20110914 天翼主副卡，副卡未激活状态标识
                        WHEN P16.On_Net_Flg='Y' THEN 1
                        WHEN P1.Std_Prd_Lvl3_Id IN (110202,110204) AND P16.On_Net_Flg IS NULL THEN 1
                        WHEN P16.On_Net_Flg='N' THEN 0 
                        WHEN CAST(TO_DATE(TO_CHAR(P1.Serv_Start_Dt) ,  'YYYYMMDD') AS CHAR(6))=CAST(TO_DATE('${TX_DATE}' ,  'YYYYMMDD') AS CHAR(6)) THEN 1
                        ELSE 0
                        END)  AS On_Net_Flg				----V1.3                         
                   ,P1.Twoway_Stop_Dt1  
                   ,P1.LATN_ID
                   ,P28.Last_Display_Area_Id
                   ,(CASE when SUBSTR(P17.VAL,TO_CHAR(P17.VAL)-3, 4)='Kbps' then P17.VAL
											 when SUBSTR(P17.VAL,TO_CHAR(P17.VAL)-3, 4)='Mbps' then CAST(CAST(SUBSTR(P17.VAL,1,TO_CHAR(P17.VAL)-4) AS INTEGER)*1024 AS VARCHAR(200))||'Kbps'
											 when SUBSTR(P17.VAL,TO_CHAR(P17.VAL),1)='M'       then CAST(CAST(SUBSTR(P17.VAL,1,TO_CHAR(P17.VAL)-1) AS INTEGER)*1024 AS VARCHAR(200))||'Kbps'
											 ELSE 'err'
											 END)  AS Speed
           FROM    MAIN_PRE_2_${LocalCode}                                 P1     
      LEFT JOIN  	 ZJBIC_1.OFR_MAIN_ASSET_FLG_${LocalCode}             			 P16           ---月表
             ON  	 P1.Asset_Row_Id=P16.Asset_Row_Id
            AND 	 P16.Bil_Month='$BILL_MONTH'                    --LEFT((CHAR(INTEGER(DATE('$TX_DATE') -1 MONTH))),6)   
      LEFT JOIN    TMP_SPEED_${LocalCode} P17
             ON    P1.ASSET_ROW_ID = P17.ASSET_ROW_ID     
      LEFT JOIN    ZJBIC_1.NEW_MARKET_AREA_NAME_${LocalCode}  P28
             ON    P1.Asset_Row_Id=P28.Asset_Row_Id  
      LEFT JOIN    NEW_BIL_FLG_${LocalCode} P2                 ----V2.0
      			 ON		 P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      



DELETE FROM KPI_1.KPI_ASSET_FLG_${LocalCode}_2
WHERE DATE_CD = TO_DATE('$TX_DATE' ,  'YYYYMMDD')
   OR (DATE_CD <= TO_DATE('$TX_DATE' ,  'YYYYMMDD') - 62 )
;   


INSERT INTO KPI_1.KPI_ASSET_FLG_${LocalCode}_2
SELECT
Asset_Row_Id
,Date_Cd
,Bil_Flg
,On_Net_Flg
,On_Serv_Flg
,Latn_Id
,Last_Display_Area_Id
,Speed
,Std_Prd_Lvl4_Id
FROM MAIN_PRE_${LocalCode}
WHERE COALESCE(Pre_Active_Status1,'-1') NOT IN ('预开户','移动即买即通') 
order by Asset_Row_Id
;




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
FROM      ZJBIC_1.OFR_TERM_USE_CUR_Z  P1
WHERE     LATN_ID = ${LATN_ID}
)
ORDER BY (Asset_Row_Id)--此表将与TMP_OFR_MAIN_ASSET_X按该字段关联，TMP_OFR_MAIN_ASSET_X为大表，且后面按该字段作分布故此处也用该字段作主索引XMX
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;




CREATE LOCAL TEMP TABLE TMP_OFR_MAIN_ASSET_${LocalCode}_2_TMP ON COMMIT PRESERVE ROWS
AS
(
select
P1.ASSET_ROW_ID
,P1.Telecom_Area_Id
FROM BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode}    P1
INNER JOIN DMN_1.CAG_COM_STD_PRD_LVL4  P12
ON P1.STD_PRD_LVL4_ID = P12.STD_PRD_LVL4_ID
AND P12.PRD_NAME='CDMA'
INNER join KPI_1.KPI_ASSET_FLG_${LocalCode}_2   P3
ON P1.ASSET_ROW_ID = P3.ASSET_ROW_ID
and P3.ON_SERV_FLG = 1
and P3.DATE_CD = TO_DATE('$TX_DATE'  , 'YYYYMMDD')  
WHERE P1.Start_Dt <= TO_DATE('$TX_DATE'  , 'YYYYMMDD')  
  and P1.End_Dt   >  TO_DATE('$TX_DATE'  , 'YYYYMMDD')  
  and P1.STAT_NAME <> '不活动'                --这些WHERE条件都来自资产主资产表的属性，所以上面的排重是不会影响这个筛选结果的，之前的排重使得这个表没有重复数据，下面可以放心使用XMX
)
ORDER BY Asset_Row_Id,Telecom_Area_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;




CREATE LOCAL TEMP TABLE TMP_OFR_MAIN_ASSET_${LocalCode}_2 ON COMMIT PRESERVE ROWS
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
FROM TMP_OFR_MAIN_ASSET_${LocalCode}_2_TMP   P1
LEFT join BSSDATA_1.OFR_ASSET_CDSC_HIST_${LocalCode}   P2
ON P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
and P2.Start_Dt <= TO_DATE('$TX_DATE'  , 'YYYYMMDD') 
and P2.End_Dt   >  TO_DATE('$TX_DATE'  , 'YYYYMMDD')
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
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;



CREATE LOCAL TEMP TABLE TMP_3G_TER_${LocalCode} ON COMMIT PRESERVE ROWS
AS
(
select 
P1.*
,P2.Telecom_Area_Id
FROM TMP_3G_TER_${LocalCode}_2   P1
INNER join TMP_OFR_MAIN_ASSET_${LocalCode}_2  P2
ON P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
and P2.Free_Flg = 0
and P2.Corp_Pay_Flg = 0
and P2.Test_Flg = 0
)
ORDER BY Asset_Row_Id,Telecom_Area_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;



DELETE FROM KPI_1.KPI_ASSET_TER_${LocalCode}_2
WHERE (DATE_CD = TO_DATE('$TX_DATE'  , 'YYYYMMDD') or DATE_CD < TO_DATE('$TX_DATE'  , 'YYYYMMDD') - 62 )
;

INSERT INTO KPI_1.KPI_ASSET_TER_${LocalCode}_2
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
TO_DATE('$TX_DATE'  , 'YYYYMMDD')
,P1.Esn_Id
,CASE WHEN NEW_ESN_FLG = 1 THEN TO_DATE('$TX_DATE'  , 'YYYYMMDD') else P3.Esn_New_Dt END 
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
LEFT join KPI_1.KPI_ASSET_TER_${LocalCode}_2  p3
ON P1.ESN_ID = P3.ESN_ID
and p3.DATE_CD = TO_DATE('$TX_DATE'  , 'YYYYMMDD') - 1 
and LATN_ID = $LATN_ID
;



CREATE LOCAL TEMP table ORDI_${LocalCode}_SPRD_MC ON COMMIT PRESERVE ROWS
AS(
SELECT     P5.Order_Item_Row_Id
             ,P5.Std_Prd_Lvl4_Id
             ,P8.Std_Prd_Lvl4_Name
     FROM    BSSDATA_1.EVT_ORDI_HIST_${LocalCode}    P5
INNER JOIN   DMN_1.CAG_COM_STD_PRD_LVL4   P8 
       ON    P5.Std_Prd_Lvl4_Id = P8.Std_Prd_Lvl4_Id 
      WHERE  P8.PRD_ID IN('40','60','50','10','70')    
      AND    P5.ETL_DT =  TO_DATE('$TX_DATE'  , 'YYYYMMDD')    
)
ORDER BY Order_Item_Row_Id,Std_Prd_Lvl4_Id
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES KSAFE 0
;      


CREATE LOCAL TEMP table ORDI_${LocalCode}_ALL_PRO ON COMMIT PRESERVE ROWS
AS(
SELECT     P1.Order_Item_Row_Id
             ,P1.Order_Row_Id
             ,P1.Stat_Name
             ,P1.CPRD_ROW_ID
             ,P1.Cpl_Dt
             ,P1.Channel_Name
             ,P1.Channel_Sub_Name
             ,P1.Sales_Emp_Id
             ,P1.Exch_Id
             ,P1.Telecom_Area_Id
             ,COALESCE(P1.Discon_Reason,'')||
                      CASE WHEN P1.Discon_Reason IS NOT NULL AND P1.Discon_Reason_Class IS NOT NULL THEN '--' 
                      ELSE '' END||
                      COALESCE(P1.Discon_Reason_Class,'') AS Discon_Reason    --YT20090204修改取数逻辑
             ,P1.CAcct_Row_Id
             ,P1.Root_Order_Item_Id
             ,P1.Latn_Id
             ,P1.Asset_Integ_Id
             ,P1.Last_Upd_Dt            
             ,(CASE P1.Action_Type_Name  WHEN '新增' THEN 'I'
                                         WHEN '拆机' THEN 'O'
                                         ELSE NULL
               END)  AS IOM_Flg
             ,P1.Pre_Active_Status
             ,P1.Prom_Asset_Integ_Id                
	           ,P2.CPRD_NAME 
             ,P2.Cprd_Id 
             ,P5.Std_Prd_Lvl4_Id
             ,P5.Std_Prd_Lvl4_Name
             ,P1.Telecom_Area_Name
     FROM    BSSDATA_1.EVT_ORDI_HIST_${LocalCode}  P1
INNER JOIN   BSSDATA_1.OFR_CPRD                    P2
       ON    P1.CPRD_ROW_ID = P2.CPrd_Row_Id
      AND    P2.Main_Child_Prd_Flg = 1
INNER JOIN   ORDI_${LocalCode}_SPRD_MC               P5                        ---YY20110705更新成LEFT JOIN 
       ON    P1.Order_Item_Row_Id = P5.Order_Item_Row_Id
    WHERE    P1.ETL_DT = TO_DATE('$TX_DATE'  , 'YYYYMMDD')  
      AND    P1.Action_Type_Name IN    ('新增','拆机') 
      AND    P1.Stat_Name = '完成'  
      AND    P1.Order_Item_Row_Id IS NOT NULL
      AND    P1.Order_Row_Id      IS NOT NULL
      AND    P1.Telecom_Area_Id   IS NOT NULL
      AND    P1.Latn_Id           IS NOT NULL     
)
ORDER BY Order_Item_Row_Id,CPRD_ROW_ID
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES KSAFE 0
; 




CREATE LOCAL TEMP table ASSET_INFO_DAILY_TMP ON COMMIT PRESERVE ROWS
AS(
SELECT 		Asset_Integ_Id
            ,Asset_Row_Id
            ,Root_Asset_Row_Id
            ,Asset_Id FROM (
SELECT      ROW_NUMBER() OVER (PARTITION BY Asset_Integ_Id ORDER BY Stat_Name DESC,Start_Dt DESC) AS Q_RANK
			,Asset_Integ_Id				  
            ,Asset_Row_Id
            ,Asset_Row_Id AS Root_Asset_Row_Id
            ,Asset_Id
 FROM       BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode}
WHERE       Start_Dt >= TO_DATE('$TX_DATE'  , 'YYYYMMDD')
  and       END_DT > TO_DATE('$TX_DATE'  , 'YYYYMMDD') - 10 
  and       Asset_Integ_Id IS not NULL
) Y  WHERE Q_RANK    = 1   
)
ORDER BY Asset_Integ_Id,Asset_Row_Id,Root_Asset_Row_Id,Asset_Id
SEGMENTED BY HASH (Asset_Integ_Id) ALL NODES KSAFE 0
;      





CREATE LOCAL TEMP table ORDI_${LocalCode}_VALID_CARD_TMP ON COMMIT PRESERVE ROWS
AS(
SELECT     
               P1.Order_Item_Row_Id
              ,P1.Order_Row_Id
              ,P1.Stat_Name
              ,P1.Last_Upd_Dt
              ,P1.CPRD_ROW_ID
              ,P1.Cpl_Dt       
              ,P1.Channel_Name
              ,P1.Channel_Sub_Name
              ,P1.Sales_Emp_Id
              ,P1.Telecom_Area_Id
              ,P1.Discon_Reason
              ,P1.CAcct_Row_Id
              ,P1.Root_Order_Item_Id
              ,P1.Latn_Id
              ,P1.Asset_Integ_Id
              ,P1.IOM_Flg
              ,P1.Std_Prd_Lvl4_Id
              ,P1.Std_Prd_Lvl4_Name  
              ,P2.Asset_Row_Id
              ,P2.Root_Asset_Row_Id 
              ,P2.Asset_Id           
              ,P1.Pre_Active_Status   
              ,P1.Prom_Asset_Integ_Id                           
	            ,P1.CPRD_NAME 
              ,P1.Cprd_Id           
              ,P1.Telecom_Area_Name
              ,P28.Last_Display_Area_Id
              ,P28.Last_Display_Area_Name
     FROM     ORDI_${LocalCode}_ALL_PRO                  P1
LEFT JOIN     ASSET_INFO_DAILY_TMP         P2           
       ON     P1.Asset_Integ_Id = P2.Asset_Integ_Id
LEFT JOIN     ZJBIC_1.NEW_MARKET_AREA_NAME_${LocalCode}  P28
      ON      P2.Asset_Row_Id=P28.Asset_Row_Id    
)
ORDER BY ASSET_ROW_ID, Order_Item_Row_Id,Asset_Integ_Id
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;      



CREATE LOCAL TEMP table ASSET_AGENT_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT       P0.ASSET_ROW_ID
            ,P1.Agent_Id     AS  AGENT_POINT_ID
            ,P2.AGENT_POINT_NAME
            ,P0.Dept_Row_Id
            ,P1.Dept_Name
     FROM   BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode}  P0 
LEFT JOIN   BSSDATA_1.PAR_DEPT                           P1
       ON   P0.Dept_Row_Id=P1.Dept_Row_Id
LEFT JOIN   BSSDATA_1.MKT_AGENT_POINT_Z      	   P2
       ON   P1.AGENT_ID=P2.AGENT_POINT_ID 
    WHERE   P0.End_Dt = DATE'3000-12-31'
      AND   P2.End_Dt = DATE'3000-12-31'
)
ORDER BY ASSET_ROW_ID,Dept_Row_Id,AGENT_POINT_ID
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;      




CREATE LOCAL TEMP table ASSET_MARKET_${LocalCode} ON COMMIT PRESERVE ROWS
AS(
SELECT       P7.Asset_Row_Id
            ,P7.Area_Id
            ,P7.Start_Dt
            ,P14.Area_Name
     FROM   BSSDATA_1.MKT_ASSET_CLAIM_${LocalCode}    P7
LEFT JOIN   BSSDATA_1.OFR_MKT_CHANNEL_${LocalCode}          P14     
       ON   P7.Area_Id = P14.Area_Id 
    WHERE   P7.Start_Dt<=TO_DATE('$TX_DATE'  , 'YYYYMMDD')
      AND   P7.End_Dt>TO_DATE('$TX_DATE'  , 'YYYYMMDD')
)
ORDER BY ASSET_ROW_ID,Area_Id 
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;      





CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE1 ON COMMIT PRESERVE ROWS
AS(
SELECT  
                   P0.Order_Item_Row_Id
                  ,P0.Order_Row_Id           
                  ,P0.Stat_Name          
                  ,P0.Last_Upd_Dt        
                  ,P0.Cpl_Dt                 
                  ,P0.Sales_Emp_Id                  
                  ,P0.Telecom_Area_Id    
                  ,P0.Discon_Reason      
                  ,P0.CAcct_Row_Id       
                  ,P0.Root_Order_Item_Id 
                  ,P0.Latn_Id            
                  ,P0.Asset_Integ_Id          
                  ,COALESCE(P7.Area_Id,-1) AS Area_Id  
	                ,P7.Start_Dt
                  ,P0.CPRD_NAME 
                  ,P0.Cprd_Id
                  ,P0.Cprd_Row_Id
                  ,COALESCE(P7.Area_Name,'err') AS Area_Name                             
                  ,P0.Telecom_Area_Name                     
                  ,P0.Asset_Row_Id                 
                  ,P18.Agent_Point_Id
                  ,P18.Agent_Point_Name 
                  ,P0.Last_Display_Area_Id
                  ,P0.Last_Display_Area_Name
         FROM     ORDI_${LocalCode}_VALID_CARD_TMP                  P0 
    LEFT JOIN     ASSET_MARKET_${LocalCode}                         P7
           ON     P0.Asset_Row_Id = P7.Asset_Row_Id      
    LEFT JOIN     ASSET_AGENT_${LocalCode}                          p18
           ON     P0.Asset_Row_Id = P18.ASSET_ROW_ID                                  
)
ORDER BY ASSET_ROW_ID,Order_Item_Row_Id,Order_Row_Id
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;      



-------CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE2 ON COMMIT PRESERVE ROWS
-------AS(
-------SELECT  
-------                   P0.Order_Item_Row_Id
-------                  ,P3.Sales_Employee_Name         
-------         FROM     ORDI_${LocalCode}_VALID_CARD_TMP                  P0 
-------    LEFT JOIN     BSSDATA_1.PAR_SALES_EMPLOYEE_HIST               P3
-------           ON     CHAR(P0.Sales_Emp_Id) = TRIM(CHAR(substr(P3.Sales_Emp_Id,7,12)))
-------          AND     SUBSTR(P3.Sales_Emp_Id,1,2)= '$LATN_ID'
-------          AND     P0.Telecom_Area_Id = SUBSTRING(P3.Sales_Emp_Id FROM 3 FOR 4)
-------          AND     P3.Start_Dt <= TO_DATE('$TX_DATE'  , 'YYYYMMDD')
-------          AND     P3.End_Dt   >  TO_DATE('$TX_DATE'  , 'YYYYMMDD')               
-------)
-------ORDER BY (Order_Item_Row_Id)
-------SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES KSAFE 0
-------;      
-------


CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE3_ORDER ON COMMIT PRESERVE ROWS
AS(
SELECT    
                   P.Order_Item_Row_Id
                  ,(CASE   
                        WHEN P1.Instant_Flg='即买即通补录'   THEN 'Y'
                        WHEN P1.Instant_Flg ='移动即买即通'  THEN 'C'                 --YT20090204 修改吴刚接口改动  增加一个C网规则
                        WHEN P.Pre_Active_Status='移动即买即通'     THEN 'C'     ---CHENK 20110922修改，移动即买即通部分改为从订单行取
                        WHEN P.Pre_Active_Status='预开户'     THEN 'C'
                        ELSE 'N'
                        END)AS Instant_Flg   
                  ,P1.APPLY_DT  Create_Dt 
                  ,P1.Order_Id  
                  ,P1.CEmployee_Row_Id   --BY WJJ                        
                  ,P1.Sales_Telecom_Area_ID                   --zn20091012订单受理营业厅  
                  ,P.IOM_Flg
                  ,P.Std_Prd_Lvl4_Id
                  ,P.Std_Prd_Lvl4_Name 
                  ,P1.Pay_Mode_Name
                  ,P1.Pre_Sales_Emp_Id
                  ,P1.Pre_Order_Id   
                  ,P.Asset_Row_Id 
                  ,P.Prom_Asset_Integ_Id                 
                 ,P1.CCust_Row_Id             --BY WJJ
                 ,P.Asset_Integ_Id    --BY WJJ
                 ,P.Last_Display_Area_Id
                 ,P.Last_Display_Area_Name
         FROM     ORDI_${LocalCode}_VALID_CARD_TMP                  P 
   INNER JOIN     BSSDATA_1.EVT_ORDER_HIST_${LocalCode}           P1       
           ON     P.Order_Row_Id = P1.Order_Row_Id                
)
ORDER BY Asset_Integ_Id,Asset_Row_Id
SEGMENTED BY HASH (Asset_Integ_Id) ALL NODES KSAFE 0
;      



CREATE LOCAL TEMP table ORDI_${LocalCode}_2_PRE3 ON COMMIT PRESERVE ROWS
AS(
SELECT    
                   P.Order_Item_Row_Id
                  ,P.Instant_Flg   
                  ,P.Create_Dt      
                  ,P.ASSET_INTEG_ID
                  --,P21.Sales_Channel_Name
                  --,P21.Sales_Channel_Sub_Name
                  --,P21.Mkt_Channel_Name
                  --,P21.Mkt_Channel_Sub_Name  
                  --,P21.Channel_Lvl2_Name      AS MKt_Channel_Lvl2_Name        --hty20110111渠道新口径
                  ,P11.CEmployee_Id          
                  ,P.Order_Id                          
                  ,P11.CEmployee_Name   
                  ,P.Sales_Telecom_Area_ID                   --zn20091012订单受理营业厅  
                  --,P13.Dept_Name   
                  ,P.CCust_Row_Id   
                  ,P.IOM_Flg
                  ,P.Std_Prd_Lvl4_Id
                  ,P.Std_Prd_Lvl4_Name 
                  ,P.Asset_Row_Id 
                  ,P.Last_Display_Area_Id
                  ,P.Last_Display_Area_Name             
          FROM    ORDI_${LocalCode}_PRE3_ORDER                  P              
    LEFT JOIN     BSSDATA_1.Par_Cemployee_Hist                    P11
           ON     P.CEmployee_Row_Id = P11.CEmployee_Row_Id
          AND     P11.Start_Dt <= TO_DATE('$TX_DATE'  , 'YYYYMMDD')
          AND     P11.End_Dt   >  TO_DATE('$TX_DATE'  , 'YYYYMMDD')            
    --LEFT JOIN     BSSDATA_1.PAR_DEPT                                  P13 
    --       ON     P.Sales_Telecom_Area_ID  = P13.Dept_Row_Id                     
    --LEFT JOIN     ZJBIC_1.ORDER_SALE_NAME_ALL_${LocalCode}            P21                 --YY20101201
    --       ON     P.Asset_Integ_Id=P21.Asset_Integ_Id
        WHERE     COALESCE(P.Instant_Flg,'Y') <> 'C'          
)
ORDER BY Order_Item_Row_Id,ASSET_INTEG_ID
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES KSAFE 0
;      




CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE3 ON COMMIT PRESERVE ROWS
AS(
SELECT    
                   P.Order_Item_Row_Id
                  ,P.Instant_Flg   
                  ,P.Create_Dt      
                  ,P.ASSET_INTEG_ID
                  ,P21.Sales_Channel_Name
                  ,P21.Sales_Channel_Sub_Name
                  ,P21.Mkt_Channel_Name
                  ,P21.Mkt_Channel_Sub_Name  
                  ,P21.Channel_Lvl2_Name      AS MKt_Channel_Lvl2_Name        --hty20110111渠道新口径
                  ,P.CEmployee_Id          
                  ,P.Order_Id                          
                  ,P.CEmployee_Name   
                  ,P.Sales_Telecom_Area_ID                  
                  ,P.CCust_Row_Id   
                  ,P.IOM_Flg
                  ,P.Std_Prd_Lvl4_Id
                  ,P.Std_Prd_Lvl4_Name 
                  ,P.Asset_Row_Id 
                  ,P.Last_Display_Area_Id
                  ,P.Last_Display_Area_Name             
          FROM    ORDI_${LocalCode}_2_PRE3                  P                                              
    LEFT JOIN     ZJBIC_1.ORDER_SALE_NAME_ALL_${LocalCode}            P21                 --YY20101201
           ON     P.ASSET_ROW_ID=P21.ASSET_ROW_ID           
)
ORDER BY Order_Item_Row_Id,Asset_Row_Id
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES KSAFE 0
;      




CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE_ALL ON COMMIT PRESERVE ROWS
AS(
SELECT         
                   P.Asset_Row_Id                   AS        Asset_Row_Id
                  ,P.Asset_Integ_Id                 AS        Asset_Integ_Id
                  ,P2.Order_Item_Row_Id              AS        Ordi_Row_Id                    
                  ,P.Order_Row_Id                   AS        Order_Row_Id                 
                  ,P2.IOM_Flg                       AS        IOM_Flg    
                  ,P.Cprd_Row_Id                    AS        Cprd_Row_Id
                  ,P.CPRD_NAME                      AS        CPrd_Name
                  ,P2.Std_Prd_Lvl4_Name             AS        Std_Prd_Lvl4_Name
                  ,P2.Std_Prd_Lvl4_Id               AS        Std_Prd_Lvl4_Id
                  ,P2.CCust_Row_Id                  AS        CCust_Row_Id
                  ,P.Telecom_Area_Id                AS        Telecom_Area_Id
                  ,P.Telecom_Area_Name              AS        Telecom_Area_Name
                  ,P.Area_Id                        AS        Area_Id
                  ,P.Area_Name                      AS        Area_Name
                  ,P2.Last_Display_Area_Id          AS        Last_Display_Area_Id   
                  ,P2.Last_Display_Area_Name        AS        Last_Display_Area_Name  
                  ,P.Agent_Point_Id                 AS        Agent_Point_Id
                  ,P.Agent_Point_Name               AS        Agent_Point_Name 
                  ,P2.Create_Dt                     AS        Apply_Dt                                            
                  ,P.Cpl_Dt                         AS        Cpl_Dt                     
                  ,P.Stat_Name                      AS        Stat_Name                 
                  ,P2.Instant_Flg                   AS        Pre_Flg                      
                  ,P2.MKt_Channel_Lvl2_Name         AS        MKt_Channel_Lvl2_Name                     
                  ,P2.Sales_Channel_Name            AS        Sales_Channel_Lvl2_Name   
                  ,P2.CEmployee_Id                  AS        Sales_Employee_Id                                                                                           
                  ,P2.CEmployee_Name                AS        Sales_Employee_Name 
                  ,P.Sales_Emp_Id                   AS        Mkt_Employee_Id 
                  ,'-1                  '           AS        Mkt_Employee_Name
                  ,P.Discon_Reason                  AS        Discon_Reason
                  ,P.Latn_Id                        AS        Latn_Id                                                              
         FROM     ORDI_${LocalCode}_PRE3                            P2 
--    LEFT JOIN     ORDI_${LocalCode}_PRE2                            P1    
--           ON     P2.Order_Item_Row_Id = P1.Order_Item_Row_Id                        
    LEFT JOIN     ORDI_${LocalCode}_PRE1                            P    
           ON     P2.Order_Item_Row_Id = P.Order_Item_Row_Id          
        WHERE     P.Order_Item_Row_Id IS NOT NULL
          AND     P.Order_Row_Id IS NOT NULL
          AND     P2.IOM_Flg IS NOT NULL
          AND     P.Stat_Name IS NOT NULL
          AND     P2.Create_Dt IS NOT NULL
          AND     P2.Std_Prd_Lvl4_Name IS NOT NULL
          AND     P.CPRD_NAME IS NOT NULL
          AND     P.Telecom_Area_Name IS NOT NULL
          AND     P2.Std_Prd_Lvl4_Id IS NOT NULL
          AND     P.Cprd_Row_Id IS NOT NULL
          AND     P.Telecom_Area_Id IS NOT NULL
          AND     Order_Id IS NOT NULL
          AND     P.Asset_Integ_Id IS NOT NULL
          AND     P2.Instant_Flg IS NOT NULL
          AND     P2.CCust_Row_Id IS NOT NULL
          AND     P.Latn_Id IS NOT NULL       
)
ORDER BY Ordi_Row_Id,Order_Item_Row_Id
SEGMENTED BY HASH (Ordi_Row_Id) ALL NODES KSAFE 0
;      



CREATE LOCAL TEMP TABLE NEW_EVT_${LocalCode} 
     (
      Asset_Row_Id                 VARCHAR(30) 
,Asset_Integ_Id                VARCHAR(30)                                                    
,Ordi_Row_Id                  VARCHAR(15)                       
,Order_Row_Id                 VARCHAR(15)                       
,IOM_Flg                      VARCHAR(1)                        
,Cprd_Row_Id                  VARCHAR(15)                       
,Cprd_Name                    VARCHAR(500)                      
,Std_Prd_Lvl4_Name            VARCHAR(100)                      
,Std_Prd_Lvl4_Id              INTEGER                           
,CCust_Row_Id                 VARCHAR(15)                                            
,Telecom_Area_Id              INTEGER                           
,Telecom_Area_Name            VARCHAR(100)                      
,Area_Id                      INTEGER                           
,Area_Name                    VARCHAR(100)                      
,Last_Display_Area_Id         DECIMAL(9,0)                      
,Last_Display_Area_Name       VARCHAR(50)                       
,Agent_Point_Id               VARCHAR(100)                      
,Agent_Point_Name             VARCHAR(100)                      
,Apply_Dt                     DATE         
,Cpl_Dt                       DATE     
--,Stat_Name                    VARCHAR(4)                 
,Stat_Name                    VARCHAR(6)                         
,Pre_Flg                      VARCHAR(10)                        
,MKt_Channel_Lvl2_Name        VARCHAR(30)                        
,Sales_Channel_Lvl2_Name      VARCHAR(30)                                               
,Sales_Employee_Id            VARCHAR(60)                        
,Sales_Employee_Name          VARCHAR(100)                       
,Mkt_Employee_Id              VARCHAR(100)                        
,Mkt_Employee_Name            VARCHAR(100)                       
,Discon_Reason                VARCHAR(300)                       
,Latn_Id                      INTEGER     
      )ON COMMIT PRESERVE ROWS
ORDER BY ( Asset_Row_Id )
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;



INSERT INTO NEW_EVT_${LocalCode}
SELECT   
               P1.ASSET_ROW_ID                                             
              ,P1.Asset_Integ_Id  
              ,'null' Ordi_Row_Id
              ,'null' Order_Row_Id
              ,'I' IOM_Flg
              ,P1.Cprd_Row_Id
              ,P6.CPrd_Name
              ,P3.Std_Prd_Lvl4_Name
              ,P1.Std_Prd_Lvl4_Id
              ,P1.CCust_Row_Id
               ,TO_NUMBER(P1.Telecom_Area_Id)
              ,P1.Telecom_Area_Name
              ,P5.Area_Id
              ,P5.Area_Name
              ,P4.Last_Display_Area_Id
              ,P4.Last_Display_Area_Name
              ,P2.Agent_Point_Id
              ,P2.Agent_Point_Name                
              ,DATE'1900-01-01' Apply_Dt
              ,P1.Serv_Start_Dt   AS Cpl_Dt
              ,'竣工'            AS Stat_Name                    
              ,'Y'  Pre_Flg                        
              ,CASE WHEN P21.Channel_Lvl2_Name IS NOT NULL THEN P21.Channel_Lvl2_Name
                    WHEN P1.DEPT_ROW_ID IS NOT NULL AND P2.Agent_Point_Id IS NULL 
                       THEN '实体渠道'
                  ELSE '社会渠道' END  Mkt_Channel_Lvl2_Name  
              ,CASE WHEN P1.DEPT_ROW_ID IS NOT NULL AND P2.Agent_Point_Id IS NULL 
                       THEN '实体渠道'
                  ELSE '社会渠道'
               END   Sales_Channel_Lvl2_Name
              ,'null' Sales_Employee_Id
              ,'null' Sales_Employee_Name
              ,'null' AS Mkt_Employee_Id
              ,'null' AS Mkt_Employee_Name 
              ,'null' AS Discon_Reason
              ,Latn_Id                                
      FROM    BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode} P1   
 LEFT JOIN    ASSET_AGENT_${LocalCode} P2
        ON    P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
INNER JOIN    DMN_1.CAG_COM_STD_PRD_LVL4 P3 
        ON    P1.STD_PRD_LVL4_ID = P3.STD_PRD_LVL4_ID
 LEFT JOIN    ZJBIC_1.NEW_MARKET_AREA_NAME_${LocalCode}  P4   
        ON    P1.Asset_Row_Id=P4.Asset_Row_Id      
 LEFT JOIN    ASSET_MARKET_${LocalCode} P5
        ON    P1.ASSET_ROW_ID = P5.ASSET_ROW_ID
 LEFT JOIN    BSSDATA_1.OFR_CPRD                    P6
        ON    P1.CPRD_ROW_ID = P6.CPrd_Row_Id
       AND    P6.Main_Child_Prd_Flg = 1
 LEFT JOIN    ZJBIC_1.ORDER_SALE_NAME_ALL_${LocalCode}            P21            
        ON    P1.ASSET_ROW_ID= P21.ASSET_ROW_ID
     WHERE    (P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  , 'YYYYMMDD') - 1 
               OR P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  , 'YYYYMMDD')
               )   
       AND    p1.Pre_Active_Status = '正常开户'  
       AND    p1.Stat_Name <> '不活动'
       and    P1.END_DT = DATE'3000-12-31'
;



-----V1.1预拆机
INSERT INTO NEW_EVT_${LocalCode}
SELECT   
               P1.ASSET_ROW_ID                                             
              ,P1.Asset_Integ_Id  
              ,'null' Ordi_Row_Id
              ,'null' Order_Row_Id
              ,'O' IOM_Flg
              ,P1.Cprd_Row_Id
              ,P6.CPrd_Name
              ,P3.Std_Prd_Lvl4_Name
              ,P1.Std_Prd_Lvl4_Id
              ,P1.CCust_Row_Id
              --,P1.Telecom_Area_Id
			  ,TO_NUMBER(P1.Telecom_Area_Id)
              ,P1.Telecom_Area_Name
              ,P5.Area_Id
              ,P5.Area_Name
              ,P4.Last_Display_Area_Id
              ,P4.Last_Display_Area_Name
              ,P2.Agent_Point_Id
              ,P2.Agent_Point_Name                
              ,DATE'1900-01-01' Apply_Dt
              ,P1.Serv_Start_Dt   AS Cpl_Dt
              ,'竣工'            AS Stat_Name                    
              ,'Y'  Pre_Flg                        
              ,CASE WHEN P21.Channel_Lvl2_Name IS NOT NULL THEN P21.Channel_Lvl2_Name
                    WHEN P1.DEPT_ROW_ID IS NOT NULL AND P2.Agent_Point_Id IS NULL 
                       THEN '实体渠道'
                  ELSE '社会渠道' END  Mkt_Channel_Lvl2_Name  
              ,CASE WHEN P1.DEPT_ROW_ID IS NOT NULL AND P2.Agent_Point_Id IS NULL 
                       THEN '实体渠道'
                  ELSE '社会渠道'
               END   Sales_Channel_Lvl2_Name
              ,'null' Sales_Employee_Id
              ,'null' Sales_Employee_Name
              ,'null' AS Mkt_Employee_Id
              ,'null' AS Mkt_Employee_Name 
              ,'null' AS Discon_Reason
              ,Latn_Id                                
      FROM    BSSDATA_1.OFR_MAIN_ASSET_HIST_${LocalCode} P1   
 LEFT JOIN    ASSET_AGENT_${LocalCode} P2
        ON    P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
INNER JOIN    DMN_1.CAG_COM_STD_PRD_LVL4 P3 
        ON    P1.STD_PRD_LVL4_ID = P3.STD_PRD_LVL4_ID
 LEFT JOIN    ZJBIC_1.NEW_MARKET_AREA_NAME_${LocalCode}  P4   
        ON    P1.Asset_Row_Id=P4.Asset_Row_Id      
 LEFT JOIN    ASSET_MARKET_${LocalCode} P5
        ON    P1.ASSET_ROW_ID = P5.ASSET_ROW_ID
 LEFT JOIN    BSSDATA_1.OFR_CPRD                    P6
        ON    P1.CPRD_ROW_ID = P6.CPrd_Row_Id
       AND    P6.Main_Child_Prd_Flg = 1
 LEFT JOIN    ZJBIC_1.ORDER_SALE_NAME_ALL_${LocalCode}            P21            
        ON    P1.ASSET_ROW_ID= P21.ASSET_ROW_ID
     WHERE    (P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  , 'YYYYMMDD') - 1 
               OR P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  , 'YYYYMMDD')
               )   
       AND    p1.pre_removed_Status IN ('D','P')  
       AND    p1.Stat_Name <> '不活动'
       and    P1.END_DT = DATE'3000-12-31'
;



INSERT INTO NEW_EVT_${LocalCode}
SELECT 
Asset_Row_Id                                                                       
,Asset_Integ_Id                                                                   
,Ordi_Row_Id                                                                      
,Order_Row_Id                                                                     
,IOM_Flg                                                                          
,Cprd_Row_Id                                                                      
,CPrd_Name                                                                        
,Std_Prd_Lvl4_Name                                                                
,Std_Prd_Lvl4_Id                                                                  
,CCust_Row_Id                                                                     
,Telecom_Area_Id                                                                  
,Telecom_Area_Name                                                                
,Area_Id                                                                          
,Area_Name                                                                        
,Last_Display_Area_Id                                                             
,Last_Display_Area_Name                                                           
,Agent_Point_Id                                                                   
,Agent_Point_Name                                                                 
,Apply_Dt                                                                         
,Cpl_Dt                                                                           
,Stat_Name                                                                   
,Pre_Flg                                                                          
,MKt_Channel_Lvl2_Name                                                            
,Sales_Channel_Lvl2_Name                                                          
,Sales_Employee_Id                                                                
,Sales_Employee_Name                                                              
,Mkt_Employee_Id                                                                  
,Mkt_Employee_Name                                                                
,Discon_Reason                                                                    
,Latn_Id                                                                          
 FROM ORDI_${LocalCode}_PRE_ALL                                  
;




CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE_ALL_CORP ON COMMIT PRESERVE ROWS
AS(
SELECT
           P1.Asset_Row_Id                
           ,P1.Asset_Integ_Id                  
           ,P1.Ordi_Row_Id                     
           ,P1.Order_Row_Id                    
           ,P1.IOM_Flg                         
           ,P1.Cprd_Row_Id                     
           ,P1.CPrd_Name
           ,P1.Std_Prd_Lvl4_Id                       
           ,P1.Std_Prd_Lvl4_Name               
           ,P1.CCust_Row_Id                    
           ,P1.Telecom_Area_Id                 
           ,P1.Telecom_Area_Name               
           ,P1.Area_Id                         
           ,P1.Area_Name                       
           ,P1.Last_Display_Area_Id            
           ,P1.Last_Display_Area_Name          
           ,P1.Agent_Point_Id                  
           ,P1.Agent_Point_Name                
           ,P1.Apply_Dt                        
           ,P1.Cpl_Dt                          
           ,P1.Stat_Name                  
           ,P1.Pre_Flg                
           ,P1.MKt_Channel_Lvl2_Name           
           ,P1.Sales_Channel_Lvl2_Name         
           ,P1.Sales_Employee_Id               
           ,P1.Sales_Employee_Name             
           ,P1.Mkt_Employee_Id                 
           ,P1.Mkt_Employee_Name               
           ,P1.Discon_Reason                   
           ,P1.Latn_Id                                        
           ,P2.CORP_USER_NAME
     FROM NEW_EVT_${LocalCode} P1
LEFT JOIN BSSDATA_1.PAR_CCUST_HIST_${LocalCode} P2
       ON P1.CCust_Row_Id = P2.CCust_Row_Id
      AND P2.Start_Dt<=TO_DATE('$TX_DATE'  , 'YYYYMMDD')
      AND P2.End_Dt>TO_DATE('$TX_DATE'  , 'YYYYMMDD')           
)
ORDER BY Asset_Row_Id,CCust_Row_Id
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      



CREATE LOCAL TEMP table TMP_SPEED_${LocalCode}_temp ON COMMIT PRESERVE ROWS          /* modified by haoyf*/
AS(
SELECT ASSET_ROW_ID,VAL FROM (
SELECT
						ROW_NUMBER() OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY 
						        CASE WHEN (P2.VAL IS NOT NULL AND P2.VAL <> '-1') THEN P2.Etl_Dt ELSE P1.Etl_Dt END DESC) AS Q_RANK
						,P1.ASSET_ROW_ID
						,CASE WHEN (P2.VAL IS NOT NULL AND P2.VAL <> '-1') THEN P2.VAL
									ELSE P1.VAL
						END VAL						
FROM 				BSSDATA_1.OFR_ASSET_EXI_HIST_${LocalCode} P1
LEFT JOIN   BSSDATA_1.OFR_ASSET_EXI_HIST_${LocalCode} P2
ON					P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
AND         P2.Start_Dt<=TO_DATE('$TX_DATE'  , 'YYYYMMDD')
AND         P2.End_Dt>TO_DATE('$TX_DATE'  , 'YYYYMMDD')
AND         P2.VAL_TYPE_NAME = '使用速率'
WHERE				P1.Start_Dt<=TO_DATE('$TX_DATE'  , 'YYYYMMDD')
AND         P1.End_Dt>TO_DATE('$TX_DATE'  , 'YYYYMMDD')
AND 				P1.VAL_TYPE_NAME = '下载速率'  
) Y  WHERE Q_RANK   = 1           
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES KSAFE 0
;      



INSERT INTO TMP_SPEED_${LocalCode}_temp
SELECT
		P1.ASSET_ROW_ID
		,CASE WHEN P1.VAL_TYPE_NAME = '速率' THEN P1.VAL
              WHEN P1.VAL_TYPE_NAME = '端口速率' THEN P1.VAL
              ELSE 'ERR'
        END VAL
FROM 				BSSDATA_1.OFR_ASSET_EXI_HIST_${LocalCode} P1
INNER JOIN  ORDI_${LocalCode}_PRE_ALL_CORP P2
        ON  P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
     WHERE	P1.Start_Dt<=TO_DATE('$TX_DATE'  , 'YYYYMMDD')
       AND  P1.End_Dt>TO_DATE('$TX_DATE'  , 'YYYYMMDD')
       AND 	P1.VAL_TYPE_NAME IN ('速率','端口速率')
       AND  P2.STD_PRD_LVL4_ID IN(14030501,14030500)
;




CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE_ALL_CORP_SPEED ON COMMIT PRESERVE ROWS
AS(
SELECT
           Asset_Row_Id                
           ,Asset_Integ_Id                  
           ,Ordi_Row_Id                     
           ,Order_Row_Id                    
           ,IOM_Flg                         
           ,Cprd_Row_Id                     
           ,CPrd_Name  
           ,Std_Prd_Lvl4_Id                     
           ,Std_Prd_Lvl4_Name               
           ,CCust_Row_Id                    
           ,Telecom_Area_Id                 
           ,Telecom_Area_Name               
           ,Area_Id                         
           ,Area_Name                       
           ,Last_Display_Area_Id            
           ,Last_Display_Area_Name       
           ,Agent_Point_Id                  
           ,Agent_Point_Name                
           ,Apply_Dt                        
           ,Cpl_Dt                          
           ,Stat_Name                  
           ,Pre_Flg                
           ,MKt_Channel_Lvl2_Name           
           ,Sales_Channel_Lvl2_Name         
           ,Sales_Employee_Id               
           ,Sales_Employee_Name             
           ,Mkt_Employee_Id                 
           ,Mkt_Employee_Name               
           ,Discon_Reason                   
           ,Latn_Id                                        
           ,CORP_USER_NAME
			,SPEED                            
          
FROM (
SELECT
            ROW_NUMBER() OVER (PARTITION BY P1.Asset_Row_Id,P1.IOM_Flg ORDER BY P1.Cpl_Dt DESC  ) AS Q_RANK
		   ,P1.Asset_Row_Id                 
           ,P1.Asset_Integ_Id                  
           ,P1.Ordi_Row_Id                     
           ,P1.Order_Row_Id                    
           ,P1.IOM_Flg                         
           ,P1.Cprd_Row_Id                     
           ,P1.CPrd_Name  
           ,P1.Std_Prd_Lvl4_Id                     
           ,P1.Std_Prd_Lvl4_Name               
           ,P1.CCust_Row_Id                    
           ,P1.Telecom_Area_Id                 
           ,P1.Telecom_Area_Name               
           ,P1.Area_Id                         
           ,P1.Area_Name                       
           ,p1.Last_Display_Area_Id            
           ,COALESCE(p1.Last_Display_Area_Name,'未落地区域') As Last_Display_Area_Name       
           ,P1.Agent_Point_Id                  
           ,P1.Agent_Point_Name                
           ,P1.Apply_Dt                        
           ,P1.Cpl_Dt                          
           ,P1.Stat_Name                  
           ,P1.Pre_Flg                
           ,P1.MKt_Channel_Lvl2_Name           
           ,P1.Sales_Channel_Lvl2_Name         
           ,P1.Sales_Employee_Id               
           ,P1.Sales_Employee_Name             
           ,P1.Mkt_Employee_Id                 
           ,P1.Mkt_Employee_Name               
           ,P1.Discon_Reason                   
           ,P1.Latn_Id                                        
           ,P1.CORP_USER_NAME											 
					 ,(CASE when SUBSTR(P17.VAL,TO_CHAR(P17.VAL)-3, 4)='Kbps' then P17.VAL
											 when SUBSTR(P17.VAL,TO_CHAR(P17.VAL)-3, 4)='Mbps' then CAST(CAST(SUBSTR(P17.VAL,1,TO_CHAR(P17.VAL)-4) AS INTEGER)*1024 AS VARCHAR(200))||'Kbps'
											 when SUBSTR(P17.VAL,TO_CHAR(P17.VAL),1)='M'       then CAST(CAST(SUBSTR(P17.VAL,1,TO_CHAR(P17.VAL)-1) AS INTEGER)*1024 AS VARCHAR(200))||'Kbps'
											 ELSE 'err'
											 END)  AS Speed						 				 
     FROM ORDI_${LocalCode}_PRE_ALL_CORP P1
LEFT JOIN TMP_SPEED_${LocalCode}_temp P17
       ON P1.ASSET_ROW_ID = P17.ASSET_ROW_ID 
---LEFT join BSSDATA_1.OFR_MKT_CHANNEL_${LocalCode} p2       ---alter by chenk 20140210 Last_Display_Area_Id为空处理，按TELECOM_AREA_ID、LATN_Id的顺序落地
---       ON P1.Telecom_Area_Id=P2.Telecom_Area_Id 
---      and p2.telecom_area_id <> -1
)Y  WHERE Q_RANK  = 1                  
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES KSAFE 0
;      


DELETE FROM KPI_1.KPI_ASSET_IO_${LocalCode}_2
WHERE  DATE_CD = TO_DATE('$TX_DATE'  , 'YYYYMMDD')
;



DELETE FROM KPI_1.KPI_ASSET_IO_${LocalCode}_2
WHERE (ASSET_ROW_ID,IO_FLG) IN (select ASSET_ROW_ID,IOM_Flg FROM ORDI_${LocalCode}_PRE_ALL_CORP_SPEED )
and ASSET_ROW_ID IS not NULL
;


INSERT INTO KPI_1.KPI_ASSET_IO_${LocalCode}_2
                (
                  Asset_Integ_Id
                 ,Asset_Row_Id
                 ,DATE_CD
                 ,Ordi_Row_Id               
                 ,Order_Row_Id
                 ,IO_Flg
                 ,Cprd_Row_Id
                 ,Cprd_Name
                 ,Std_Prd_Lvl4_Id
                 ,Std_Prd_Lvl4_Name                 
                 ,CCust_Row_Id
                 ,Corp_User_Name
                 ,Telecom_Area_Id
                 ,Telecom_Area_Name
                 ,Area_Id
                 ,Area_Name
                 ,Last_Display_Area_Id
                 ,Last_Display_Area_Name        
                 ,Agent_Point_Id  
                 ,Agent_Point_Name
                 ,Apply_Dt
                 ,Cpl_Dt
                 ,Stat_Name
                 ,Pre_Flg
                 ,MKt_Channel_Lvl2_Name
                 ,Sales_Channel_Lvl2_Name
                 ,Lan_Speed
                 ,Sales_Employee_Id
                 ,Sales_Employee_Name
                 ,Mkt_Employee_Id 
                 ,Mkt_Employee_Name 
                 ,Discon_Reason  
                 ,Prom_Row_Id
                 ,Std_Prom_Lvl4_Id      
             )
          SELECT 
                 P1.Asset_Integ_Id
                 ,P1.Asset_Row_Id
                 ,TO_DATE('$TX_DATE'  , 'YYYYMMDD') DATE_CD  
                 ,P1.Ordi_Row_Id               
                 ,P1.Order_Row_Id
                 ,P1.IOM_Flg AS IO_Flg
                 ,P1.Cprd_Row_Id
                 ,P1.Cprd_Name
                 ,P1.Std_Prd_Lvl4_Id
                 ,P1.Std_Prd_Lvl4_Name                 
                 ,P1.CCust_Row_Id
                 ,P1.Corp_User_Name
                 ,P1.Telecom_Area_Id
                 ,P1.Telecom_Area_Name
                 ,P1.Area_Id
                 ,P1.Area_Name
                 ,P1.Last_Display_Area_Id
                 ,P1.Last_Display_Area_Name        
                 ,P1.Agent_Point_Id  
                 ,P1.Agent_Point_Name
                 ,P1.Apply_Dt
                 ,P1.Cpl_Dt
                 ,P1.Stat_Name
                 ,P1.Pre_Flg
                 ,P1.MKt_Channel_Lvl2_Name
                 ,P1.Sales_Channel_Lvl2_Name
                 ,P1.SPEED AS Lan_Speed
                 ,P1.Sales_Employee_Id
                 ,P1.Sales_Employee_Name
                 ,P1.Mkt_Employee_Id 
                 ,P1.Mkt_Employee_Name 
                 ,P1.Discon_Reason  
                 ,P2.Prom_Row_Id
                 ,P2.Std_Prom_Lvl4_Id  
       FROM ORDI_${LocalCode}_PRE_ALL_CORP_SPEED  p1
  LEFT join BSSDATA_1.OFR_ASSET_PROM_INTEG_HIST_${LocalCode}   P2
         ON P1.ASSET_ROW_ID =P2.ASSET_ROW_ID
        and P2.START_DT<=TO_DATE('$TX_DATE'  , 'YYYYMMDD') 
        AND P2.END_DT>TO_DATE('$TX_DATE'  , 'YYYYMMDD')         
;




DELETE FROM KPI_1.KPI_ASSET_IO_${LocalCode}_2
WHERE Ordi_Row_Id IN (select ROOT_ORDER_ITEM_ID FROM BSSDATA_1.EVT_ORDI_HIST_${LocalCode} WHERE ETL_DT = TO_DATE('$TX_DATE' ,  'YYYYMMDD')  
and CPRD_ROW_ID = '3-1JLGG7P'  )
;


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

sub main
{
   my $ret;

   #open(LOGONFILE_H, "${LOGON_FILE}");
   #$LOGON_STR = <LOGONFILE_H>;
   #close(LOGONFILE_H);

   # Get the decoded logon string
   #$LOGON_STR = `${AUTO_HOME}/bin/IceCode.exe "$LOGON_STR"`;

   # Call bteq command to load data
   $ret = run_bteq_command();
   print "run_bteq_command() = $ret\n";
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



