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

$hostname = "v001";
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

--------------资产主要信息


--CREATE LOCAL TEMP table OFR_MAIN_ASSET_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table OFR_MAIN_ASSET_A ON COMMIT PRESERVE ROWS
AS(
SELECT     P1.Asset_Row_Id
 
           --,TO_DATE('$TX_DATE'  ,'YYYYMMDD') AS Start_Dt
           ,TO_DATE('20170807'  ,'YYYYMMDD') AS Start_Dt
           
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

	--FROM  BSSVIEW.OFR_MAIN_ASSET_HIST_${LocalCode}   P1
	--INNER JOIN  DMNVIEW.CAG_COM_STD_PRD_LVL4                  P12
      FROM  BSSDATA.OFR_MAIN_ASSET_HIST_A   P1
     INNER JOIN  DMN.CAG_COM_STD_PRD_LVL4                  P12
	 
        ON  P1.Std_Prd_Lvl4_Id=P12.Std_Prd_Lvl4_Id
		
     -- WHERE  P1.Start_Dt<=TO_DATE('$TX_DATE'  ,'YYYYMMDD')
	 --   AND  P1.End_Dt  >TO_DATE('$TX_DATE'  ,'YYYYMMDD')
     WHERE  P1.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
	     AND  P1.End_Dt  >TO_DATE('20170807'  ,'YYYYMMDD')	
		 
	     AND  P12.PRD_ID IN('40','60','50','10','70')   --CDMA\ITV\宽带\普通电话
	     --AND  (P1.stat_name <>'不活动'  OR  (stat_name ='不活动' and start_dt>=DATE('$TX_DATE') -5 DAYS) )  
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


-------------测试用户标记
--CREATE LOCAL TEMP table MAIN_FREE_CORP_PAY_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table MAIN_FREE_CORP_PAY_A ON COMMIT PRESERVE ROWS
AS(
select   Asset_Row_Id
            ,MAX(CASE WHEN P.Cdsc_Row_Id='1-19849-1' THEN 1 ELSE 0 END) AS Test_Flg
			
      --FROM  BSSVIEW.OFR_ASSET_CDSC_HIST_${LocalCode}       P  
      FROM  BSSDATA.OFR_ASSET_CDSC_HIST_A      P  
	  
     WHERE  P.Stat_Name ='使用中'
	 
       --AND  P.Start_Dt<=TO_DATE('$TX_DATE'  ,'YYYYMMDD')
       --AND  P.End_Dt>TO_DATE('$TX_DATE' ,'YYYYMMDD')
       --AND  P.Asset_Row_Id IN (SELECT Asset_Row_Id FROM OFR_MAIN_ASSET_${LocalCode})
       AND  P.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
       AND  P.End_Dt>TO_DATE('20170107'  ,'YYYYMMDD')
       AND  P.Asset_Row_Id IN (SELECT Asset_Row_Id FROM OFR_MAIN_ASSET_A)
	   
      GROUP BY Asset_Row_Id 
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;

---------------停复机流水信息

--CREATE LOCAL TEMP table STOP_ASSET_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table STOP_ASSET_A ON COMMIT PRESERVE ROWS
AS(
    SELECT  Asset_Row_Id
        		,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P') THEN 1 else 0 END) AS Twoway_Stop_Flg   ----F0K（双停）    ----V2.1    
    		    ,MAX(CASE WHEN Call_Limit_Stat='F0M' THEN 1 else 0 END) AS Oneway_Stop_Flg   ----F0M（单停）  
            ,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P')                                         ----F0K（双停）    ----V2.1 			
                     
                      THEN TO_DATE('19990101' , 'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Deal_Time) , 'YYYYMMDD')
                  END ) AS Oneway_Stop_Dt
            ,MAX(CASE WHEN Call_Limit_Stat='F0M'                                        ----F0M（单停）			
                      
                      THEN TO_DATE('19990101' , 'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Deal_Time) , 'YYYYMMDD')
                  END ) AS Twoway_Stop_Dt
				  
      --FROM  ${SOURCEDB}.OFR_ASSET_CALL_LIMIT_LOG_${LocalCode}
      --WHERE  TO_DATE(TO_CHAR(Deal_Time) , 'YYYYMMDD')<=TO_DATE('${TX_DATE}'  ,'YYYYMMDD')
      FROM  BSSDATA.OFR_ASSET_CALL_LIMIT_LOG_A
     WHERE  TO_DATE(TO_CHAR(Deal_Time) , 'YYYYMMDD')<=TO_DATE('20170807'  ,'YYYYMMDD')
	 
       AND  Call_Limit_Stat IN('F0M','F0K','F0P')                                        ----V4.5 
     GROUP BY Asset_Row_Id
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


--CREATE LOCAL TEMP table STOP_ASSET_1_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table STOP_ASSET_1_A ON COMMIT PRESERVE ROWS
AS(
    SELECT  Asset_Row_Id
        		,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P') THEN 1 else 0 END) AS Twoway_Stop_Flg1      ----V2.1 
    		    ,MAX(CASE WHEN Call_Limit_Stat='F0M' THEN 1 else 0 END) AS Oneway_Stop_Flg1                
            ,MAX(CASE WHEN Call_Limit_Stat IN('F0K','F0P')                                             ----V2.1 
                      
                      THEN TO_DATE('19990101' , 'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Stat_Upd_Time), 'YYYYMMDD')
                  END ) AS Oneway_Stop_Dt1
            ,MAX(CASE WHEN Call_Limit_Stat='F0M' 
                      
                      THEN TO_DATE('19990101' , 'YYYYMMDD')
                      ELSE TO_DATE(TO_CHAR(Stat_Upd_Time), 'YYYYMMDD')
                  END ) AS Twoway_Stop_Dt1
				  
     -- FROM  ${SOURCEDB}.OFR_ASSET_CALL_LIMIT_LOG_${LocalCode}
     --WHERE  TO_DATE(TO_CHAR(Stat_Upd_Time), 'YYYYMMDD')<=TO_DATE('${TX_DATE}'  ,'YYYYMMDD')
     FROM  BSSDATA.OFR_ASSET_CALL_LIMIT_LOG_A
     WHERE  TO_DATE(TO_CHAR(Stat_Upd_Time), 'YYYYMMDD')<=TO_DATE('20170807'  ,'YYYYMMDD')
       AND  Deal_Time IS NULL
       AND  Call_Limit_Stat IN('F0M','F0K','F0P')
     GROUP BY Asset_Row_Id
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


--CREATE LOCAL TEMP table OWE_92_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table OWE_92_A ON COMMIT PRESERVE ROWS
AS(
SELECT  p.Asset_Integ_Id            ----------alter by chenk 20130121,更改关联字段，ASSET_INTEG_ID-->ASSET_ROW_ID
            ,p1.ASSET_ROW_ID
            ,SUM(Amt)   Amt

--     FROM  BSSVIEW.FIN_PG_OWE_STMT_ITEM_D_${LocalCode} p
--LEFT JOIN  BSSVIEW.OFR_MAIN_ASSET_HIST_${LocalCode} P1
			FROM  BSSDATA.FIN_PG_OWE_STMT_ITEM_D_A p
LEFT JOIN  BSSDATA.OFR_MAIN_ASSET_HIST_A P1
       ON  P.Asset_Integ_Id=P1.Asset_Integ_Id
      AND  P1.END_DT=DATE'3000-12-31'
	  
	-- WHERE  Stmt_Dt <= TO_DATE('$TX_DATE'  ,'YYYYMMDD')- 91   
    WHERE  Stmt_Dt <= TO_DATE('20170807'  ,'YYYYMMDD')- 91 
      AND  FLG='1'
  GROUP BY  p.Asset_Integ_Id
            ,p1.ASSET_ROW_ID
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


--CREATE LOCAL TEMP table MAIN_PRE_1_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table MAIN_PRE_1_A ON COMMIT PRESERVE ROWS
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
			  
/*
        FROM  OFR_MAIN_ASSET_${LocalCode}                        P1
   LEFT JOIN  STOP_ASSET_${LocalCode}                           P34
          ON  P1.Asset_Row_Id=P34.Asset_Row_Id
   LEFT JOIN  STOP_ASSET_1_${LocalCode}                         P33
          ON  P1.Asset_Row_Id=P33.Asset_Row_Id       
   LEFT JOIN  MAIN_FREE_CORP_PAY_${LocalCode}                   P18
          ON  P1.Asset_Row_Id=P18.Asset_Row_Id
   LEFT JOIN  OWE_92_${LocalCode}              P2
          ON  P1.Asset_Row_Id = P2.Asset_Row_Id
         AND  P2.Amt > 0
**/
 	  FROM  OFR_MAIN_ASSET_A                            P1
   LEFT JOIN  STOP_ASSET_A                           P34
          ON  P1.Asset_Row_Id=P34.Asset_Row_Id
   LEFT JOIN  STOP_ASSET_1_A                        P33
          ON  P1.Asset_Row_Id=P33.Asset_Row_Id       
   LEFT JOIN  MAIN_FREE_CORP_PAY_A                   P18
          ON  P1.Asset_Row_Id=P18.Asset_Row_Id
   LEFT JOIN  OWE_92_A              P2
          ON  P1.Asset_Row_Id = P2.Asset_Row_Id
         AND  P2.Amt > 0
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES;


--CREATE LOCAL TEMP table MAIN_PRE_2_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table MAIN_PRE_2_A ON COMMIT PRESERVE ROWS
AS(
SELECT  P1.*
                 ,(CASE WHEN P1.Std_Prd_Lvl4_Id IN (11010200,11010201,11010202,11010307) AND P20.Stat_Name IS NOT NULL AND P20.Stat_Name IN ('IPAS激活','CRM预占')     THEN '正常开户'
                        WHEN P1.Std_Prd_Lvl4_Id IN (11010200,11010201,11010202,11010307) AND P20.Stat_Name IS NOT NULL AND P20.Stat_Name NOT IN ('IPAS激活','CRM预占') THEN '预开户'
                        ELSE P1.Pre_Active_Status
                   END) AS Pre_Active_Status1
				   
	   -- FROM    MAIN_PRE_1_${LocalCode}                      P1             
		  FROM    MAIN_PRE_1_A                                 P1   
		  
      LEFT JOIN   (SELECT  Asset_Row_Id
                          ,MIN(Stat_Name) AS Stat_Name
						  
                    --FROM  BSSVIEW.OFR_ASSET_INST_STAT_HIST_${LocalCode}
                    FROM  BSSDATA.OFR_ASSET_CDSC_HIST_A
					
                   WHERE  Asset_Row_Id IS NOT NULL
                     AND  Asset_Row_Id <> '-1'
					 
					 --AND  Start_Dt<=TO_DATE('$TX_DATE'  ,'YYYYMMDD')
                     --AND  End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD')
                     AND  Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
                     AND  End_Dt>TO_DATE('20170107'  ,'YYYYMMDD')
                 GROUP BY Asset_Row_Id)                                              P20
             ON  	 P1.Asset_Row_Id=P20.Asset_Row_Id   
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


-------------速率

--CREATE LOCAL TEMP TABLE TMP_SPEED_${LocalCode} ON COMMIT PRESERVE ROWS AS
CREATE LOCAL TEMP TABLE TMP_SPEED_A ON COMMIT PRESERVE ROWS AS
(
    SELECT
        ASSET_ROW_ID,
        VAL
    FROM
        (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY
                CASE
                    WHEN (P2.VAL IS NOT NULL
                        AND P2.VAL <> '-1')
                    THEN P2.Etl_Dt
                    ELSE P1.Etl_Dt
                END DESC) = 1 AS RANK ,
                P1.ASSET_ROW_ID ,
                CASE
                    WHEN (P2.VAL IS NOT NULL
                        AND P2.VAL <> '-1')
                    THEN P2.VAL
                    ELSE P1.VAL
                END VAL
            FROM
			    --BSSVIEW.OFR_ASSET_EXI_HIST_${LocalCode} P1
                BSSDATA.OFR_ASSET_EXI_HIST_A P1
            LEFT JOIN
				--BSSVIEW.OFR_ASSET_EXI_HIST_${LocalCode} P2
                BSSDATA.OFR_ASSET_EXI_HIST_A P2
            ON
                P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
				
			--AND P2.Start_Dt<=TO_DATE('$TX_DATE','YYYYMMDD')
            --AND P2.End_Dt>TO_DATE('$TX_DATE' ,'YYYYMMDD')
            AND P2.Start_Dt<=TO_DATE('20170807' ,'YYYYMMDD')
            AND P2.End_Dt>TO_DATE('20170107' ,'YYYYMMDD')
            AND P2.VAL_TYPE_NAME = '使用速率'
            WHERE
			
			--P1.Start_Dt<=TO_DATE('$TX_DATE' ,'YYYYMMDD')
            --AND P1.End_Dt>TO_DATE('$TX_DATE','YYYYMMDD')
                P1.Start_Dt<=TO_DATE('20170807' ,'YYYYMMDD')
            AND P1.End_Dt>TO_DATE('20170107' ,'YYYYMMDD')
            AND P1.VAL_TYPE_NAME = '下载速率') T
    WHERE
        RANK =1 ) 
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;      


-------------插入ITV速率        ------------------V2.2

--INSERT INTO TMP_SPEED_${LocalCode}
INSERT INTO TMP_SPEED_A
SELECT ASSET_ROW_ID, VAL FROM (
SELECT ROW_NUMBER() OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY CASE WHEN P1.VAL_TYPE_NAME = '速率' THEN 2
                                                                         WHEN P1.VAL_TYPE_NAME = '端口速率' THEN 1
                                                                    else 0 END DESC) AS RANK
						,P1.ASSET_ROW_ID
						,CASE WHEN P1.VAL_TYPE_NAME = '速率' THEN P1.VAL
                  WHEN P1.VAL_TYPE_NAME = '端口速率' THEN P1.VAL
                ELSE 'ERR'
            END VAL
FROM 				
		--BSSVIEW.OFR_ASSET_EXI_HIST_${LocalCode} P1
		BSSDATA.OFR_ASSET_EXI_HIST_A P1
INNER JOIN  
		--MAIN_PRE_2_${LocalCode} P2
		MAIN_PRE_2_A P2
        ON  P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
	
	---WHERE	P1.Start_Dt<TO_DATE('$TX_DATE' ,'YYYYMMDD')
     --  AND  P1.End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD') 
     WHERE	P1.Start_Dt<TO_DATE('20170807'  ,'YYYYMMDD')
       AND  P1.End_Dt>TO_DATE('20170807'  ,'YYYYMMDD') 
	   
       AND 	P1.VAL_TYPE_NAME IN ('速率','端口速率')
       AND  P2.STD_PRD_LVL4_ID IN(14030501,14030500)
) T WHERE RANK = 1       
;


-------------V2.0集团统一计费口径

--CREATE LOCAL TEMP TABLE OWE_STMT_1_${LocalCode}  ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP TABLE OWE_STMT_1_A  ON COMMIT PRESERVE ROWS
AS 
(select   					P1.ASSET_ROW_ID--V2.7 改用Asset_row_id关联
                    ,P1.Owe_Stmt_Dt
                    ,P1.Stmt_Dt
                    ,SUM(P1.AMT) AS OWE_AMT      ----按照资产集成编号进行
       
	   --FROM   BSSVIEW.FIN_PG_OWE_STMT_ITEM_D_${LocalCode}   P1
       --INNER JOIN		OFR_MAIN_ASSET_${LocalCode} P2       
	   FROM   BSSDATA.FIN_PG_OWE_STMT_ITEM_D_A   P1
       INNER JOIN		OFR_MAIN_ASSET_A P2
	   
       				 ON		P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
       				AND   P2.STAT_NAME <> '不活动'
            WHERE   P1.FLG IN ('1','2')
              AND   P1.Bad_Debt_Flg = 'N'
   GROUP BY 1,2,3
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


--CREATE LOCAL TEMP table OWE_STMT_2_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table OWE_STMT_2_A ON COMMIT PRESERVE ROWS
AS 
(select             ASSET_ROW_ID
                    ,MAX(CAST(EXTRACT(YEAR FROM Owe_Stmt_Dt) - EXTRACT(YEAR FROM Stmt_Dt) AS INTEGER)*12+CAST(EXTRACT(MONTH FROM Owe_Stmt_Dt)-(EXTRACT(MONTH FROM Stmt_Dt)+1) AS INTEGER)) NBR
					
	  	 -- FROM    OWE_STMT_1_${LocalCode} 
            FROM    OWE_STMT_1_A 
			
           WHERE    OWE_AMT > 0
           GROUP BY 1
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


--CREATE LOCAL TEMP TABLE NEW_BIL_FLG_${LocalCode}_1 ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP TABLE NEW_BIL_FLG_A_1 ON COMMIT PRESERVE ROWS
AS
(
		SELECT
							P1.ASSET_ROW_ID
							,CASE WHEN P1.STAT_NAME = '不活动' THEN 0
										WHEN P1.Pre_Active_Status ='预开户' THEN 0
										WHEN P1.STD_PRD_LVL4_ID IN ('11020413','11020419') AND P1.STAT_NAME = '已暂停' 
										
										--AND P1.STAT_CHANGE_DT < TO_DATE('$TX_DATE' , 'YYYYMMDD')-31 THEN 0
										AND P1.STAT_CHANGE_DT < TO_DATE('20170807'  ,'YYYYMMDD')-31 THEN 0
										WHEN P1.STD_PRD_LVL4_ID NOT IN ('11020413','11020419') AND P2.ASSET_ROW_ID IS NOT NULL AND P2.NBR >= 3 THEN 0
										ELSE 1
							END AS BIL_FLG
							
			--FROM		OFR_MAIN_ASSET_${LocalCode} P1
			--LEFT JOIN		OWE_STMT_2_${LocalCode} P2
			FROM		OFR_MAIN_ASSET_A P1
			LEFT JOIN		OWE_STMT_2_A P2
 				ON		P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


----V4.0取出限制外网资产数据

--CREATE LOCAL TEMP TABLE NEW_BIL_FLG_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP TABLE NEW_BIL_FLG_A ON COMMIT PRESERVE ROWS
AS
( SELECT ASSET_ROW_ID,BIL_FLG FROM(
		SELECT ROW_NUMBER () OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY BIL_FLG)  AS RANK
							,P1.ASSET_ROW_ID
							,CASE WHEN P2.ASSET_ROW_ID IS NOT NULL THEN 0 ELSE P1.BIL_FLG END BIL_FLG
			FROM		
				--NEW_BIL_FLG_${LocalCode}_1 P1
				NEW_BIL_FLG_A_1 P1
	LEFT JOIN   
				--BSSVIEW.OFR_CHILD_ASSET_HIST_${LocalCode} P2
				BSSDATA.OFR_CHILD_ASSET_HIST_A P2
        ON    P1.ASSET_ROW_ID = P2.ROOT_ASSET_ROW_ID
		
	  --AND    P2.START_DT <= TO_DATE('${TX_DATE}' ,'YYYYMMDD')
      --AND    P2.End_Dt > TO_DATE('${TX_DATE}'  ,'YYYYMMDD')
       AND    P2.START_DT <= TO_DATE('20170807'  ,'YYYYMMDD')
       AND    P2.End_Dt > TO_DATE('20170107'  ,'YYYYMMDD')
       AND    P2.CPRD_ROW_ID = '1-HKOIMR0'
       AND    P2.STAT_NAME <> '不活动'
)T WHERE RANK = 1
)
ORDER BY (ASSET_ROW_ID)  
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;




--CREATE LOCAL TEMP table MAIN_PRE_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table MAIN_PRE_A ON COMMIT PRESERVE ROWS
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
												  
												  -- AND P1.TwoWay_stop_Dt <= TO_DATE('$TX_DATE'  ,'YYYYMMDD')-62
                                                  AND P1.TwoWay_stop_Dt <= TO_DATE('20170807'  ,'YYYYMMDD')-62  
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
												  
												  -- AND P1.TwoWay_stop_Dt <= TO_DATE('$TX_DATE'   ,'YYYYMMDD')-62 
                                                  AND P1.TwoWay_stop_Dt <= TO_DATE('20170807'  ,'YYYYMMDD')-62  
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
                        WHEN CAST(TO_DATE(TO_CHAR(P1.Serv_Start_Dt),'YYYYMMDD') AS CHAR(6))=CAST(TO_DATE('20170807'  ,'YYYYMMDD') AS CHAR(6)) THEN 1
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

/*
           FROM    MAIN_PRE_2_${LocalCode}                                 P1     
      LEFT JOIN  	 ZJBIC.OFR_MAIN_ASSET_FLG_${LocalCode}             			 P16           ---月表
             ON  	 P1.Asset_Row_Id=P16.Asset_Row_Id
            AND 	 P16.Bil_Month='$BILL_MONTH'                    --LEFT((CHAR(INTEGER(DATE('$TX_DATE') -1 MONTH))),6)   
      LEFT JOIN    TMP_SPEED_${LocalCode} P17
             ON    P1.ASSET_ROW_ID = P17.ASSET_ROW_ID     
      LEFT JOIN    ZJBIC.NEW_MARKET_AREA_NAME_${LocalCode}  P28
             ON    P1.Asset_Row_Id=P28.Asset_Row_Id  
      LEFT JOIN    NEW_BIL_FLG_${LocalCode} P2                 ----V2.0
      			 ON		 P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
**/
		   FROM    MAIN_PRE_2_A                                 P1     
      LEFT JOIN  	 ZJBIC.OFR_MAIN_ASSET_FLG_A             			 P16           ---月表
             ON  	 P1.Asset_Row_Id=P16.Asset_Row_Id
            AND 	 P16.Bil_Month='$BILL_MONTH'                    --LEFT((CHAR(INTEGER(DATE('$TX_DATE') -1 MONTH))),6)   
      LEFT JOIN    TMP_SPEED_A P17
             ON    P1.ASSET_ROW_ID = P17.ASSET_ROW_ID     
      LEFT JOIN    ZJBIC.NEW_MARKET_AREA_NAME_A  P28
             ON    P1.Asset_Row_Id=P28.Asset_Row_Id  
      LEFT JOIN    NEW_BIL_FLG_A P2                 ----V2.0
      			 ON		 P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;      


--DELETE FROM KPI.KPI_ASSET_FLG_${LocalCode}
--WHERE DATE_CD = TO_DATE('$TX_DATE'  ,'YYYYMMDD')
--   OR (DATE_CD <= TO_DATE('$TX_DATE'  ,'YYYYMMDD') - 62 )
DELETE FROM KPI.KPI_ASSET_FLG_A
WHERE DATE_CD = TO_DATE('20170807'  ,'YYYYMMDD')
   OR (DATE_CD <= TO_DATE('20170807'  ,'YYYYMMDD') - 62 )
;   


--INSERT INTO KPI.KPI_ASSET_FLG_${LocalCode}
INSERT INTO KPI.KPI_ASSET_FLG_A
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

--FROM MAIN_PRE_${LocalCode}
FROM MAIN_PRE_A
WHERE COALESCE(Pre_Active_Status1,'-1') NOT IN ('预开户','移动即买即通') 
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



