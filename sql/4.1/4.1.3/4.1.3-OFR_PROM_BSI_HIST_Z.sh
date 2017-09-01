#!/bin/bash  
######################################################################################
# load STAGE_1.OFR_PROM_BSI_HIST_Z to 30
running=0
parrel=1
invoke=0

date
echo "start loading OFR_PROM_BSI_HIST_Z"

while (($invoke < =31))  
do   
	running=`ps -ef|grep "vsql -h"|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 31) ;j++ ))
 	do	
		
		####${VSQL} -e -i -v ON_ERROR_STOP=1 -c "truncate table ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke};"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY STAGE_1.OFR_PROM_BSI_HIST_Z FROM '/data/SOURCE/biz_data3/STAGE_OFR_PROM_BSI_HIST_Z_${invoke}001.dat' ON v_poc_node0002 DELIMITER E'\007' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.3.STAGE_1.OFR_PROM_BSI_HIST_Z${invoke}.log 2>&1  & 
		
		echo "invoke OFR_MAIN_ASSET_HIST_A_${invoke}"
		let "invoke += 1"
	done	
	
	sleep 3 
	
done


while (($running > 0))
do
	running=`ps -ef|grep "vsql -h"|grep -v grep |wc -l`
        sleep 2
done

date
echo "load OFR_MAIN_ASSET_CUR completed"


