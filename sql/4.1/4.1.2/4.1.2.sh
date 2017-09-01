#!/bin/bash  
######################################################################################
#parameters
RESOURCEPOOL="exporting_pool"
export VSQL_HOME=/tmp/pools/${RESOURCEPOOL}
mkdir -p ${VSQL_HOME}
echo "set session resource_pool=${RESOURCEPOOL};" > ${VSQL_HOME}/.vsqlrc

# export ZJBIC.OFR_MAIN_ASSET_CUR_A_1 to 30
running=0
parrel=4
invoke=1

date
echo "start exporting OFR_MAIN_ASSET_CUR"

while (($invoke <= 30))  
do   
	running=`ps -ef|grep "exportdata("|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 30) ;j++ ))
 	do	
		
		echo "start exporting OFR_MAIN_ASSET_CUR_${invoke}"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "Select exportdata(*  using parameters path='/data/export/OFR_MAIN_ASSET_CUR/ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke}.txt', separator=E'\007') over (partition auto) from  ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke};" > ${AUTO_HOME}/logs/4.1.2_ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke}.log 2>&1  & 
		
		echo "invoke OFR_MAIN_ASSET_CUR_${invoke}"
		let "invoke += 1"
	done	
	
	sleep 3 
	
done


while (($running > 0))
do
	running=`ps -ef|grep "exportdata("|grep -v grep |wc -l`
        sleep 3
done

date
echo "export OFR_MAIN_ASSET_CUR completed"

######################################################################################
# export TPCDS.CATALOG_SALES_1 to 30


running=0
parrel=6
invoke=1

date
echo "start exporting CATALOG_SALES"

while (($invoke <= 30))  
do   
	running=`ps -ef|grep "exportdata("|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 30) ;j++ ))
 	do	
		
		echo "start exporting CATALOG_SALES_${invoke}"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "select exportdata(cs_sold_date_sk ,    cs_sold_time_sk ,    cs_ship_date_sk ,    cs_bill_customer_sk ,    cs_bill_cdemo_sk ,    cs_bill_hdemo_sk ,    cs_bill_addr_sk ,    cs_ship_customer_sk ,    cs_ship_cdemo_sk ,    cs_ship_hdemo_sk ,    cs_ship_addr_sk ,    cs_call_center_sk ,    cs_catalog_page_sk ,    cs_ship_mode_sk ,    cs_warehouse_sk ,    cs_item_sk ,    cs_promo_sk ,    cs_order_number ,    cs_quantity ,    cs_wholesale_cost ::varchar,    cs_list_price ::varchar,    cs_sales_price ::varchar,    cs_ext_discount_amt ::varchar,    cs_ext_sales_price ::varchar,    cs_ext_wholesale_cost ::varchar,    cs_ext_list_price ::varchar,    cs_ext_tax ::varchar,    cs_coupon_amt ::varchar,    cs_ext_ship_cost ::varchar,    cs_net_paid ::varchar,    cs_net_paid_inc_tax ::varchar,    cs_net_paid_inc_ship ::varchar,    cs_net_paid_inc_ship_tax ::varchar,    cs_net_profit ::varchar  using parameters path='/data/export/CATALOG_SALES/TPCDS.CATALOG_SALES_${invoke}.txt', separator=E'\007') over (partition auto) from  TPCDS.CATALOG_SALES_${invoke};" > ${AUTO_HOME}/logs/4.1.2_TPCDS.CATALOG_SALES_${invoke}.log 2>&1  & 
		
		echo "invoke CATALOG_SALES_${invoke}"
		let "invoke += 1"
	done	
	
	sleep 3 
	
done


while (($running > 0))
do
	running=`ps -ef|grep "exportdata("|grep -v grep |wc -l`
        sleep 3
done

date
echo "export CATALOG_SALES completed"
o "export CATALOG_SALES completed"
