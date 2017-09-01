#! /bin/bash

echo "4.2.2 HDFS数据装载功能测试"

# set resource pool
RESOURCEPOOL="adhoc_query_pool"
export VSQL_HOME=/tmp/pools/${RESOURCEPOOL}
mkdir -p ${VSQL_HOME}
echo "set session resource_pool=${RESOURCEPOOL};" > ${VSQL_HOME}/.vsqlrc
 

echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


    vsql -h 134.96.238.231 -U dbadmin -w dbadmin -f /home/dbadmin/ZJTELCOM_POC/sql/4.2/4.2.2.sql     


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}

exit
