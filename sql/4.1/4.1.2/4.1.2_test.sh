#!/bin/bash  
######################################################################################
# export ZJBIC.OFR_MAIN_ASSET_CUR_A_1 to 30
running=0
parrel=4
invoke=1

date
echo "start exporting OFR_MAIN_ASSET_CUR"

while (($invoke < 30))  
do   
	running=`ps -ef|grep "ZJBIC"|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 30) ;j++ ))
 	do	
		
		echo "start exporting OFR_MAIN_ASSET_CUR_${invoke}"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "select * from  ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke} limit 1;" > ${AUTO_HOME}/logs/4.1.2_ZJBIC.OFR_MAIN_ASSET_CUR_A_${invoke}.log 2>&1  & 
		
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

