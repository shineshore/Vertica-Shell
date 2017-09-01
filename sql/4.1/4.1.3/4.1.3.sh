#! /bin/bash  
######################################################################################
# load KPI_ASSET_FLG_A_ 0 to 9
running=0
parrel=1
invoke=0

date
LETTER=(A C D F G)
for (let in ${LETTER[*]})
do
echo "start loading KPI_ASSET_FLG_${let}_"

while (($invoke < 10))  
do   
	running=`ps -ef|grep "vsql -h"|grep -v grep |wc -l`
	
	for (( j=1;(j <= ($parrel - $running)) && ($invoke <= 9) ;j++ ))
 	do	
		
		#${VSQL} -e -i -v ON_ERROR_STOP=1 -c "truncate table KPI_1.KPI_ASSET_FLG_${let}_1${invoke};"
		${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY KPI_1.KPI_ASSET_FLG_${let}_1 FROM '/data/SOURCE/biz_data2/KPI_KPI_ASSET_FLG_${let}_${invoke}001.dat' ON v_poc_node0002 DELIMITER E'\007' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.1_KPI_1.KPI_ASSET_FLG_${let}_1_${invoke}.log 2>&1  & 
		#${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY KPI_1.KPI_ASSET_FLG_${let}_2 FROM '/data/SOURCE/biz_data2/KPI_KPI_ASSET_FLG_${let}_${invoke}001.dat' ON v_poc_node0002 DELIMITER #E'\007' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.1_KPI_1.KPI_ASSET_FLG_${let}_2_${invoke}.log 2>&1  & 
		#${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY KPI_2.KPI_ASSET_FLG_${let}_3 FROM '/data/SOURCE/biz_data2/KPI_KPI_ASSET_FLG_${let}_${invoke}001.dat' ON v_poc_node0002 DELIMITER #E'\007' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.1_KPI_2.KPI_ASSET_FLG_${let}_3_${invoke}.log 2>&1  & 
		#${VSQL} -e -i -v ON_ERROR_STOP=1 -c "COPY KPI_2.KPI_ASSET_FLG_${let}_4 FROM '/data/SOURCE/biz_data2/KPI_KPI_ASSET_FLG_${let}_${invoke}001.dat' ON v_poc_node0002 DELIMITER #E'\007' DIRECT RejectMax 10;" > ${AUTO_HOME}/logs/4.1.1_KPI_2.KPI_ASSET_FLG_${let}_4_${invoke}.log 2>&1  & 
		
		echo "invoke OFR_MAIN_ASSET_CUR_${invoke}"
		let "invoke += 1"
	done	
	
	sleep 3 
	
done
done


while (($running > 0))
do
	running=`ps -ef|grep "vsql -h"|grep -v grep |wc -l`
        sleep 3
done

date
echo "load OFR_MAIN_ASSET_CUR completed"

######################################################################################

