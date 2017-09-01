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

--CREATE LOCAL TEMP table ORDI_${LocalCode}_SPRD_MC ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_SPRD_MC ON COMMIT PRESERVE ROWS
AS(
SELECT     P5.Order_Item_Row_Id
             ,P5.Std_Prd_Lvl4_Id
             ,P8.Std_Prd_Lvl4_Name
			 
	 --FROM    BSSVIEW.EVT_ORDI_HIST_${LocalCode}    P5
     FROM    BSSDATA.EVT_ORDI_HIST_A    P5
INNER JOIN   DMN.CAG_COM_STD_PRD_LVL4   P8 
       ON    P5.Std_Prd_Lvl4_Id = P8.Std_Prd_Lvl4_Id 
      WHERE  P8.PRD_ID IN('40','60','50','10','70')   
	  
	  --AND    P5.ETL_DT =  TO_DATE('$TX_DATE' ,'YYYYMMDD')
      AND    P5.ETL_DT =  TO_DATE('20170807'  ,'YYYYMMDD')    
)
ORDER BY (Order_Item_Row_Id)
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES
;      


--CREATE LOCAL TEMP table ORDI_${LocalCode}_ALL_PRO ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_ALL_PRO ON COMMIT PRESERVE ROWS
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
			 
     --FROM    BSSVIEW.EVT_ORDI_HIST_${LocalCode}  P1
	 FROM    BSSDATA.EVT_ORDI_HIST_A  P1
INNER JOIN   BSSDATA.OFR_CPRD                    P2
       ON    P1.CPRD_ROW_ID = P2.CPrd_Row_Id
      AND    P2.Main_Child_Prd_Flg = 1
	  
--INNER JOIN   ORDI_${LocalCode}_SPRD_MC               P5                        ---YY20110705更新成LEFT JOIN 
INNER JOIN   ORDI_A_SPRD_MC               P5                        ---YY20110705更新成LEFT JOIN 
       ON    P1.Order_Item_Row_Id = P5.Order_Item_Row_Id
	   
	--WHERE    P1.ETL_DT = TO_DATE('$TX_DATE' ,'YYYYMMDD') 
    WHERE    P1.ETL_DT = TO_DATE('20170807'  ,'YYYYMMDD')  
      AND    P1.Action_Type_Name IN    ('新增','拆机') 
      AND    P1.Stat_Name = '完成'  
      AND    P1.Order_Item_Row_Id IS NOT NULL
      AND    P1.Order_Row_Id      IS NOT NULL
      AND    P1.Telecom_Area_Id   IS NOT NULL
      AND    P1.Latn_Id           IS NOT NULL     
)
ORDER BY (Order_Item_Row_Id)
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES
; 



--CREATE VOLATILE MULTISET table ASSET_INFO_DAILY_TMP, NO LOG
CREATE LOCAL TEMP table ASSET_INFO_DAILY_TMP ON COMMIT PRESERVE ROWS
AS(
SELECT  Asset_Integ_Id
            ,Asset_Row_Id
            ,Root_Asset_Row_Id
            ,Asset_Id
FROM (            
SELECT      ROW_NUMBER() OVER (PARTITION BY Asset_Integ_Id ORDER BY Stat_Name DESC,Start_Dt DESC) AS RANK
            ,Asset_Integ_Id
            ,Asset_Row_Id
            ,Asset_Row_Id AS Root_Asset_Row_Id
            ,Asset_Id

--  FROM       BSSVIEW.OFR_MAIN_ASSET_HIST_${LocalCode}
--WHERE       Start_Dt >= TO_DATE('$TX_DATE'  ,'YYYYMMDD')
--  and       END_DT > TO_DATE('$TX_DATE'  ,'YYYYMMDD') - 10
 FROM       BSSDATA.OFR_MAIN_ASSET_HIST_A
WHERE       Start_Dt >= TO_DATE('20170807'  ,'YYYYMMDD')
  and       END_DT > TO_DATE('20170807'  ,'YYYYMMDD') - 10 
  
  and       Asset_Integ_Id IS not NULL
) T WHERE RANK     = 1  
)
ORDER BY (Asset_Integ_Id)
SEGMENTED BY HASH (Asset_Integ_Id) ALL NODES
;      




--CREATE LOCAL TEMP table ORDI_${LocalCode}_VALID_CARD_TMP ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_VALID_CARD_TMP ON COMMIT PRESERVE ROWS
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
			  
     --FROM     ORDI_${LocalCode}_ALL_PRO                  P1
	 FROM     ORDI_A_ALL_PRO                  P1
LEFT JOIN     ASSET_INFO_DAILY_TMP         P2           
       ON     P1.Asset_Integ_Id = P2.Asset_Integ_Id
	   
--LEFT JOIN     ZJBIC.NEW_MARKET_AREA_NAME_${LocalCode}  P28
LEFT JOIN     ZJBIC.NEW_MARKET_AREA_NAME_A  P28
      ON      P2.Asset_Row_Id=P28.Asset_Row_Id    
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES
;      


--CREATE LOCAL TEMPtable ASSET_AGENT_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ASSET_AGENT_A ON COMMIT PRESERVE ROWS
AS(
SELECT       P0.ASSET_ROW_ID
            ,P1.Agent_Id     AS  AGENT_POINT_ID
            ,P2.AGENT_POINT_NAME
            ,P0.Dept_Row_Id
            ,P1.Dept_Name
			
     --FROM   BSSVIEW.OFR_MAIN_ASSET_HIST_${LocalCode}  P0
	 FROM   BSSDATA.OFR_MAIN_ASSET_HIST_A  P0 
LEFT JOIN   BSSDATA.PAR_DEPT                           P1
       ON   P0.Dept_Row_Id=P1.Dept_Row_Id
LEFT JOIN   BSSDATA.MKT_AGENT_POINT_Z      	   P2
       ON   P1.AGENT_ID=P2.AGENT_POINT_ID 
    WHERE   P0.End_Dt = DATE'3000-12-31'
      AND   P2.End_Dt = DATE'3000-12-31'
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES
;      



--CREATE LOCAL TEMP table ASSET_MARKET_${LocalCode} ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ASSET_MARKET_A ON COMMIT PRESERVE ROWS
AS(
SELECT       P7.Asset_Row_Id
            ,P7.Area_Id
            ,P7.Start_Dt
            ,P14.Area_Name
/*			
	FROM   BSSVIEW.MKT_ASSET_CLAIM_${LocalCode}    P7
LEFT JOIN   BSSVIEW.OFR_MKT_CHANNEL_${LocalCode}          P14     
       ON   P7.Area_Id = P14.Area_Id 
    WHERE   P7.Start_Dt<=TO_DATE('$TX_DATE'  ,'YYYYMMDD')
      AND   P7.End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD')
**/
     FROM   BSSDATA.MKT_ASSET_CLAIM_A    P7
LEFT JOIN   BSSDATA.OFR_MKT_CHANNEL_A          P14     
       ON   P7.Area_Id = P14.Area_Id 
    WHERE   P7.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
      AND   P7.End_Dt>TO_DATE('20170107'  ,'YYYYMMDD')
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES
;      



--CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE1 ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_PRE1 ON COMMIT PRESERVE ROWS
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
				  
	/*FROM     ORDI_${LocalCode}_VALID_CARD_TMP                  P0 
    LEFT JOIN     ASSET_MARKET_${LocalCode}                         P7
           ON     P0.Asset_Row_Id = P7.Asset_Row_Id      
    LEFT JOIN     ASSET_AGENT_${LocalCode}                          p18
           ON     P0.Asset_Row_Id = P18.ASSET_ROW_ID 
    */
    FROM     ORDI_A_VALID_CARD_TMP                  P0 
    LEFT JOIN     ASSET_MARKET_A                         P7
           ON     P0.Asset_Row_Id = P7.Asset_Row_Id      
    LEFT JOIN     ASSET_AGENT_A                          p18
           ON     P0.Asset_Row_Id = P18.ASSET_ROW_ID                                  
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES
;      



-------CREATE LOCAL TEMP table ORDI_A_PRE2 ON COMMIT PRESERVE ROWS
-------AS(
-------SELECT  
-------                   P0.Order_Item_Row_Id
-------                  ,P3.Sales_Employee_Name         
-------         FROM     ORDI_A_VALID_CARD_TMP                  P0 
-------    LEFT JOIN     BSSDATA.PAR_SALES_EMPLOYEE_HIST               P3
-------           ON     CHAR(P0.Sales_Emp_Id) = TRIM(CHAR(substr(P3.Sales_Emp_Id,7,12)))
-------          AND     SUBSTR(P3.Sales_Emp_Id,1,2)= '$LATN_ID'
-------          AND     P0.Telecom_Area_Id = SUBSTRING(P3.Sales_Emp_Id FROM 3 FOR 4)
-------          AND     P3.Start_Dt <= TO_DATE('20170807'  ,'YYYYMMDD')
-------          AND     P3.End_Dt   >  TO_DATE('20170807'  ,'YYYYMMDD')               
-------)
-------ORDER BY (Order_Item_Row_Id)
-------ON COMMIT PRESERVE ROWS
-------;      
-------.IF ERRORCODE <> 0 THEN .GOTO QUITWITHERROR;

--CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE3_ORDER ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_PRE3_ORDER ON COMMIT PRESERVE ROWS
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
	/*
	FROM     ORDI_${LocalCode}_VALID_CARD_TMP                  P 
   INNER JOIN     BSSVIEW.EVT_ORDER_HIST_${LocalCode}           P1       
           ON     P.Order_Row_Id = P1.Order_Row_Id 
	**/
         FROM     ORDI_A_VALID_CARD_TMP                  P 
   INNER JOIN     BSSDATA.EVT_ORDER_HIST_A           P1       
           ON     P.Order_Row_Id = P1.Order_Row_Id                
)
ORDER BY (Asset_Integ_Id)
SEGMENTED BY HASH (Asset_Integ_Id) ALL NODES
;      


--CREATE LOCAL TEMP table ORDI_${LocalCode}_1_PRE3 ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_1_PRE3 ON COMMIT PRESERVE ROWS
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
     /*
		 FROM    ORDI_${LocalCode}_PRE3_ORDER                  P              
    LEFT JOIN     BSSVIEW.Par_Cemployee_Hist                    P11
           ON     P.CEmployee_Row_Id = P11.CEmployee_Row_Id
          AND     P11.Start_Dt <= TO_DATE('$TX_DATE'  ,'YYYYMMDD')
          AND     P11.End_Dt   >  TO_DATE('$TX_DATE'  ,'YYYYMMDD') 
	 **/
		  FROM    ORDI_A_PRE3_ORDER                  P              
    LEFT JOIN     BSSDATA.Par_Cemployee_Hist                    P11
           ON     P.CEmployee_Row_Id = P11.CEmployee_Row_Id
          AND     P11.Start_Dt <= TO_DATE('20170807'  ,'YYYYMMDD')
          AND     P11.End_Dt   >  TO_DATE('20170107'  ,'YYYYMMDD')            
    --LEFT JOIN     BSSDATA.PAR_DEPT                                  P13 
    --       ON     P.Sales_Telecom_Area_ID  = P13.Dept_Row_Id                     
    --LEFT JOIN     ZJBIC.ORDER_SALE_NAME_ALL_${LocalCode}            P21                 --YY20101201
    --       ON     P.Asset_Integ_Id=P21.Asset_Integ_Id
        WHERE     COALESCE(P.Instant_Flg,'Y') <> 'C'          
) 
ORDER BY (Order_Item_Row_Id)
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES
;     



--CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE3 ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_PRE3 ON COMMIT PRESERVE ROWS
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
/*
	 FROM    ORDI_${LocalCode}_1_PRE3                  P                                              
    LEFT JOIN     ZJBIC.ORDER_SALE_NAME_ALL_${LocalCode}            P21                 --YY20101201
           ON     P.ASSET_ROW_ID=P21.ASSET_ROW_ID
**/       
	   FROM    ORDI_A_1_PRE3                  P                                              
    LEFT JOIN     ZJBIC.ORDER_SALE_NAME_ALL_A            P21                 --YY20101201
           ON     P.ASSET_ROW_ID=P21.ASSET_ROW_ID           
)
ORDER BY (Order_Item_Row_Id)
SEGMENTED BY HASH (Order_Item_Row_Id) ALL NODES
;      



--CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE_ALL ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_PRE_ALL ON COMMIT PRESERVE ROWS
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
				  
         --FROM     ORDI_${LocalCode}_PRE3                            P2
		 FROM     ORDI_A_PRE3                            P2 
		 
--    LEFT JOIN     ORDI_${LocalCode}_PRE2                            P1    
--           ON     P2.Order_Item_Row_Id = P1.Order_Item_Row_Id 

	--LEFT JOIN     ORDI_${LocalCode}_PRE1                            P                         
    LEFT JOIN     ORDI_A_PRE1                            P    
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
ORDER BY (Ordi_Row_Id)
SEGMENTED BY HASH (Ordi_Row_Id) ALL NODES
;      



--DROP TABLE NEW_EVT_A ;

--CREATE LOCAL TEMP TABLE NEW_EVT_${LocalCode} 
CREATE LOCAL TEMP TABLE NEW_EVT_A 
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
      ) ON COMMIT PRESERVE ROWS
ORDER BY ( Asset_Row_Id )
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;


--INSERT INTO NEW_EVT_${LocalCode}
INSERT INTO NEW_EVT_A
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
/*
      FROM    BSSVIEW.OFR_MAIN_ASSET_HIST_${LocalCode} P1   
 LEFT JOIN    ASSET_AGENT_${LocalCode} P2
        ON    P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
INNER JOIN    DMNVIEW.CAG_COM_STD_PRD_LVL4 P3 
        ON    P1.STD_PRD_LVL4_ID = P3.STD_PRD_LVL4_ID
 LEFT JOIN    ZJBIC.NEW_MARKET_AREA_NAME_${LocalCode}  P4   
        ON    P1.Asset_Row_Id=P4.Asset_Row_Id      
 LEFT JOIN    ASSET_MARKET_${LocalCode} P5
        ON    P1.ASSET_ROW_ID = P5.ASSET_ROW_ID
 LEFT JOIN    BSSVIEW.OFR_CPRD                    P6
        ON    P1.CPRD_ROW_ID = P6.CPrd_Row_Id
       AND    P6.Main_Child_Prd_Flg = 1
 LEFT JOIN    ZJBIC.ORDER_SALE_NAME_ALL_${LocalCode}            P21            
        ON    P1.ASSET_ROW_ID= P21.ASSET_ROW_ID
     WHERE    (P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  ,'YYYYMMDD') - 1 
               OR P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  ,'YYYYMMDD')
               )   
       AND    p1.Pre_Active_Status = '正常开户'  
       AND    p1.Stat_Name <> '不活动'
       and    P1.END_DT = DATE'3000-12-31'
**/

			  FROM    BSSDATA.OFR_MAIN_ASSET_HIST_A P1   
 LEFT JOIN    ASSET_AGENT_A P2
        ON    P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
INNER JOIN    DMN.CAG_COM_STD_PRD_LVL4 P3 
        ON    P1.STD_PRD_LVL4_ID = P3.STD_PRD_LVL4_ID
 LEFT JOIN    ZJBIC.NEW_MARKET_AREA_NAME_A  P4   
        ON    P1.Asset_Row_Id=P4.Asset_Row_Id      
 LEFT JOIN    ASSET_MARKET_A P5
        ON    P1.ASSET_ROW_ID = P5.ASSET_ROW_ID
 LEFT JOIN    BSSDATA.OFR_CPRD                    P6
        ON    P1.CPRD_ROW_ID = P6.CPrd_Row_Id
       AND    P6.Main_Child_Prd_Flg = 1
 LEFT JOIN    ZJBIC.ORDER_SALE_NAME_ALL_A            P21            
        ON    P1.ASSET_ROW_ID= P21.ASSET_ROW_ID
     WHERE    (P1.Serv_Start_Dt = TO_DATE('20170807'  ,'YYYYMMDD') - 1 
               OR P1.Serv_Start_Dt = TO_DATE('20170107'  ,'YYYYMMDD')
               )   
       AND    p1.Pre_Active_Status = '正常开户'  
       AND    p1.Stat_Name <> '不活动'
       and    P1.END_DT = DATE'3000-12-31'
;



-----V1.1预拆机
--INSERT INTO NEW_EVT_${LocalCode}
INSERT INTO NEW_EVT_A
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
/*
      FROM    BSSVIEW.OFR_MAIN_ASSET_HIST_${LocalCode} P1   
 LEFT JOIN    ASSET_AGENT_${LocalCode} P2
        ON    P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
INNER JOIN    DMNVIEW.CAG_COM_STD_PRD_LVL4 P3 
        ON    P1.STD_PRD_LVL4_ID = P3.STD_PRD_LVL4_ID
 LEFT JOIN    ZJBIC.NEW_MARKET_AREA_NAME_${LocalCode}  P4   
        ON    P1.Asset_Row_Id=P4.Asset_Row_Id      
 LEFT JOIN    ASSET_MARKET_${LocalCode} P5
        ON    P1.ASSET_ROW_ID = P5.ASSET_ROW_ID
 LEFT JOIN    BSSVIEW.OFR_CPRD                    P6
        ON    P1.CPRD_ROW_ID = P6.CPrd_Row_Id
       AND    P6.Main_Child_Prd_Flg = 1
 LEFT JOIN    ZJBIC.ORDER_SALE_NAME_ALL_${LocalCode}            P21            
        ON    P1.ASSET_ROW_ID= P21.ASSET_ROW_ID
     WHERE    (P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  ,'YYYYMMDD') - 1 
               OR P1.Serv_Start_Dt = TO_DATE('$TX_DATE'  ,t'YYYYMMDD')
               )   
       AND    p1.pre_removed_Status IN ('D','P')  
       AND    p1.Stat_Name <> '不活动'
       and    P1.END_DT = DATE'3000-12-31'
**/

			  FROM    BSSDATA.OFR_MAIN_ASSET_HIST_A P1   
 LEFT JOIN    ASSET_AGENT_A P2
        ON    P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
INNER JOIN    DMN.CAG_COM_STD_PRD_LVL4 P3 
        ON    P1.STD_PRD_LVL4_ID = P3.STD_PRD_LVL4_ID
 LEFT JOIN    ZJBIC.NEW_MARKET_AREA_NAME_A  P4   
        ON    P1.Asset_Row_Id=P4.Asset_Row_Id      
 LEFT JOIN    ASSET_MARKET_A P5
        ON    P1.ASSET_ROW_ID = P5.ASSET_ROW_ID
 LEFT JOIN    BSSDATA.OFR_CPRD                    P6
        ON    P1.CPRD_ROW_ID = P6.CPrd_Row_Id
       AND    P6.Main_Child_Prd_Flg = 1
 LEFT JOIN    ZJBIC.ORDER_SALE_NAME_ALL_A            P21            
        ON    P1.ASSET_ROW_ID= P21.ASSET_ROW_ID
     WHERE    (P1.Serv_Start_Dt = TO_DATE('20170807'  ,'YYYYMMDD') - 1 
               OR P1.Serv_Start_Dt = TO_DATE('20170807'  ,'YYYYMMDD')
               )   
       AND    p1.pre_removed_Status IN ('D','P')  
       AND    p1.Stat_Name <> '不活动'
       and    P1.END_DT = DATE'3000-12-31'
;


--INSERT INTO NEW_EVT_${LocalCode}
INSERT INTO NEW_EVT_A
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
-- FROM ORDI_${LocalCode}_PRE_ALL                                                                    
 FROM ORDI_A_PRE_ALL                                  
;


--CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE_ALL_CORP ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_PRE_ALL_CORP ON COMMIT PRESERVE ROWS
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
/*
     FROM NEW_EVT_${LocalCode} P1
LEFT JOIN BSSVIEW.PAR_CCUST_HIST_${LocalCode} P2
       ON P1.CCust_Row_Id = P2.CCust_Row_Id
      AND P2.Start_Dt<=TO_DATE('$TX_DATE' ,'YYYYMMDD')
      AND P2.End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD') 
**/	  

     FROM NEW_EVT_A P1
LEFT JOIN BSSDATA.PAR_CCUST_HIST_A P2
       ON P1.CCust_Row_Id = P2.CCust_Row_Id
      AND P2.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
      AND P2.End_Dt>TO_DATE('20170107'  ,'YYYYMMDD')           
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (Asset_Row_Id) ALL NODES
;      



CREATE LOCAL TEMP table TMP_SPEED_A ON COMMIT PRESERVE ROWS
AS(
SELECT ASSET_ROW_ID,VAL FROM(

SELECT  
						ROW_NUMBER() OVER (PARTITION BY P1.ASSET_ROW_ID ORDER BY 
						        CASE WHEN (P2.VAL IS NOT NULL AND P2.VAL <> '-1') THEN P2.Etl_Dt ELSE P1.Etl_Dt END DESC) AS BANK
						,P1.ASSET_ROW_ID
						,CASE WHEN (P2.VAL IS NOT NULL AND P2.VAL <> '-1') THEN P2.VAL
									ELSE P1.VAL
						END VAL						

/*
FROM 				BSSVIEW.OFR_ASSET_EXI_HIST_${LocalCode} P1
LEFT JOIN   BSSVIEW.OFR_ASSET_EXI_HIST_${LocalCode} P2
ON					P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
AND         P2.Start_Dt<=TO_DATE('$TX_DATE' ,'YYYYMMDD')
AND         P2.End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD')
AND         P2.VAL_TYPE_NAME = '使用速率'
WHERE				P1.Start_Dt<=TO_DATE('$TX_DATE'  ,'YYYYMMDD')
AND         P1.End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD')
AND 				P1.VAL_TYPE_NAME = '下载速率'
**/						
FROM 				BSSDATA.OFR_ASSET_EXI_HIST_A P1
LEFT JOIN   BSSDATA.OFR_ASSET_EXI_HIST_A P2
ON					P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
AND         P2.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
AND         P2.End_Dt>TO_DATE('20170807'  ,'YYYYMMDD')
AND         P2.VAL_TYPE_NAME = '使用速率'
WHERE				P1.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
AND         P1.End_Dt>TO_DATE('20170807'  ,'YYYYMMDD')
AND 				P1.VAL_TYPE_NAME = '下载速率'  
) T WHERE BANK = 1           
)
ORDER BY (ASSET_ROW_ID)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES
;      


--INSERT INTO TMP_SPEED_${LocalCode}
INSERT INTO TMP_SPEED_A
SELECT
						P1.ASSET_ROW_ID
						,CASE WHEN P1.VAL_TYPE_NAME = '速率' THEN P1.VAL
                  WHEN P1.VAL_TYPE_NAME = '端口速率' THEN P1.VAL
                ELSE 'ERR'
            END VAL
/*
FROM 				BSSVIEW.OFR_ASSET_EXI_HIST_${LocalCode} P1
INNER JOIN  ORDI_${LocalCode}_PRE_ALL_CORP P2
        ON  P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
     WHERE	P1.Start_Dt<=TO_DATE('$TX_DATE'  ,'YYYYMMDD')
       AND  P1.End_Dt>TO_DATE('$TX_DATE'  ,'YYYYMMDD')
       AND 	P1.VAL_TYPE_NAME IN ('速率','端口速率')
       AND  P2.STD_PRD_LVL4_ID IN(14030501,14030500)
**/			
			FROM 				BSSDATA.OFR_ASSET_EXI_HIST_A P1
INNER JOIN  ORDI_A_PRE_ALL_CORP P2
        ON  P1.ASSET_ROW_ID = P2.ASSET_ROW_ID
     WHERE	P1.Start_Dt<=TO_DATE('20170807'  ,'YYYYMMDD')
       AND  P1.End_Dt>TO_DATE('20170107'  ,'YYYYMMDD')
       AND 	P1.VAL_TYPE_NAME IN ('速率','端口速率')
       AND  P2.STD_PRD_LVL4_ID IN(14030501,14030500)
;



--CREATE LOCAL TEMP table ORDI_${LocalCode}_PRE_ALL_CORP_SPEED ON COMMIT PRESERVE ROWS
CREATE LOCAL TEMP table ORDI_A_PRE_ALL_CORP_SPEED ON COMMIT PRESERVE ROWS
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
           ,COALESCE(Last_Display_Area_Name,'未落地区域') As Last_Display_Area_Name       
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
           ,SPEED FROM (
SELECT     ROW_NUMBER() OVER (PARTITION BY P1.Asset_Row_Id,P1.IOM_Flg ORDER BY P1.Cpl_Dt DESC  ) AS RANK      
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
/*
     FROM ORDI_${LocalCode}_PRE_ALL_CORP P1
LEFT JOIN TMP_SPEED_${LocalCode} P17
       ON P1.ASSET_ROW_ID = P17.ASSET_ROW_ID 
**/

	FROM ORDI_A_PRE_ALL_CORP P1
LEFT JOIN TMP_SPEED_A P17
       ON P1.ASSET_ROW_ID = P17.ASSET_ROW_ID 
---LEFT join BSSDATA.OFR_MKT_CHANNEL_${LocalCode} p2       ---alter by chenk 20140210 Last_Display_Area_Id为空处理，按TELECOM_AREA_ID、LATN_Id的顺序落地
---       ON P1.Telecom_Area_Id=P2.Telecom_Area_Id 
---      and p2.telecom_area_id <> -1
) T WHERE RANK = 1                  
)
ORDER BY (Asset_Row_Id)
SEGMENTED BY HASH (ASSET_ROW_ID) ALL NODES
;      


--DELETE FROM KPI.KPI_ASSET_IO_${LocalCode}
--WHERE  DATE_CD = TO_DATE('$TX_DATE'  ,t'YYYYMMDD')
DELETE FROM KPI.KPI_ASSET_IO_A
WHERE  DATE_CD = TO_DATE('20170807'  ,'YYYYMMDD')
;


--DELETE FROM KPI.KPI_ASSET_IO_${LocalCode}
--WHERE (ASSET_ROW_ID,IO_FLG) IN (select ASSET_ROW_ID,IOM_Flg FROM ORDI_${LocalCode}_PRE_ALL_CORP_SPEED )
--and ASSET_ROW_ID IS not NULL

DELETE FROM KPI.KPI_ASSET_IO_A
WHERE (ASSET_ROW_ID,IO_FLG) IN (select ASSET_ROW_ID,IOM_Flg FROM ORDI_A_PRE_ALL_CORP_SPEED )
and ASSET_ROW_ID IS not NULL
;


--INSERT INTO KPI.KPI_ASSET_IO_${LocalCode}
INSERT INTO KPI.KPI_ASSET_IO_A
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
				 --,TO_DATE('$TX_DATE'  ,'YYYYMMDD') DATE_CD
                 ,TO_DATE('20170807'  ,'YYYYMMDD') DATE_CD  
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
/*
       FROM ORDI_${LocalCode}_PRE_ALL_CORP_SPEED  p1
  LEFT join BSSVIEW.OFR_ASSET_PROM_INTEG_HIST_${LocalCode}   P2
         ON P1.ASSET_ROW_ID =P2.ASSET_ROW_ID
        and P2.START_DT<=TO_DATE('$TX_DATE'  ,'YYYYMMDD') 
        AND P2.END_DT>TO_DATE('$TX_DATE'  ,'YYYYMMDD') 
**/


	  FROM ORDI_A_PRE_ALL_CORP_SPEED  p1
  LEFT join BSSDATA.OFR_ASSET_PROM_INTEG_HIST_A   P2
         ON P1.ASSET_ROW_ID =P2.ASSET_ROW_ID
        and P2.START_DT<=TO_DATE('20170807'  ,'YYYYMMDD') 
        AND P2.END_DT>TO_DATE('20170107'  ,'YYYYMMDD')         
;

/*
DELETE FROM KPI.KPI_ASSET_IO_${LocalCode}
WHERE Ordi_Row_Id IN (select ROOT_ORDER_ITEM_ID FROM BSSVIEW.EVT_ORDI_HIST_${LocalCode} WHERE ETL_DT = TO_DATE('$TX_DATE' , 'YYYYMMDD')  
and CPRD_ROW_ID = '3-1JLGG7P'  )
**/

DELETE FROM KPI.KPI_ASSET_IO_A
WHERE Ordi_Row_Id IN (select ROOT_ORDER_ITEM_ID FROM BSSDATA.EVT_ORDI_HIST_A WHERE ETL_DT = TO_DATE('20170807'  ,'YYYYMMDD')  
and CPRD_ROW_ID = '3-1JLGG7P'  )
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



