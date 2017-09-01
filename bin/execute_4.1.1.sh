#!/bin/bash  

echo "4.1.1 数据加载性能测试"

######################################################################################
#parameters
RESOURCEPOOL="loading_pool"
export VSQL_HOME=/tmp/pools/${RESOURCEPOOL}
mkdir -p ${VSQL_HOME}
echo "set session resource_pool=${RESOURCEPOOL};" > ${VSQL_HOME}/.vsqlrc
#vsql -h 134.96.238.231 -U dbadmin -w dbadmin -c "show resource_pool ;" 


# load ZJBIC.OFR_MAIN_ASSET_CUR_A_1 to 30
running=0
parrel=10
invoke=1

date
echo "start loading OFR_MAIN_ASSET_CUR"

while (($invoke <= 30))  
do   
	running=`ps -ef|grep "COPY"|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 30) ;j++ ))
 	do	
		echo "start loading OFR_MAIN_ASSET_CUR_${invoke}"	
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "truncate table ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke};"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke} FROM '/data/SOURCE/case4.1.1/OFR_MAIN_ASSET_CUR_A_20170731_T_01.dat' ON ANY NODE DELIMITER E'\007' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.1_ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke}.log 2>&1  & 
		
		echo "invoke OFR_MAIN_ASSET_CUR_${invoke}"
		let "invoke += 1"
	done	
	
	sleep 3 
	
done


while (($running > 0))
do
	running=`ps -ef|grep "COPY"|grep -v grep |wc -l`
        sleep 3
done

date
echo "load OFR_MAIN_ASSET_CUR completed"

######################################################################################
# load TPCDS.CATALOG_SALES_1 to 30


running=0
parrel=10
invoke=1

date
echo "start loading CATALOG_SALES"

while (($invoke <= 30))  
do   
	running=`ps -ef|grep "COPY"|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 30) ;j++ ))
 	do	
		
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "truncate table TPCDS.CATALOG_SALES_${invoke};"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY TPCDS.CATALOG_SALES_${invoke} FROM '/data/SOURCE/case4.1.1/catalog_sales_01.dat' ON ANY NODE DELIMITER '|' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.1_TPCDS.CATALOG_SALES_${invoke}.log 2>&1  & 
		
		echo "invoke CATALOG_SALES_${invoke}"
		let "invoke += 1"
	done	
	
	sleep 3 
	
done


while (($running > 0))
do
	running=`ps -ef|grep "COPY"|grep -v grep |wc -l`
        sleep 3
done

date
echo "load CATALOG_SALES completed"

