#!/bin/bash

echo "4.1.4 并发40查询测试"

# set resource pool
RESOURCEPOOL="concurrent_query_pool"
export VSQL_HOME=/tmp/pools/${RESOURCEPOOL}
mkdir -p ${VSQL_HOME}
echo "set session resource_pool=${RESOURCEPOOL};" > ${VSQL_HOME}/.vsqlrc
#vsql -h 134.96.238.231 -U dbadmin -w dbadmin -c "show resource_pool ;" 


echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


    perl ~/ZJTELCOM_POC/bin/paralle.pl ~/ZJTELCOM_POC/sql/4.1/4.1.4/4.1.4_con_40.cfg     


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}

exit
