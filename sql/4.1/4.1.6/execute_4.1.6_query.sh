#!/bin/bash

echo "4.1.6 TPCDS查询测试"

# set resource pool
#RESOURCEPOOL="concurrent_query_pool_10"
#export VSQL_HOME=/tmp/pools/$(basename $(dirname $0))_${RESOURCEPOOL}
#mkdir -p ${VSQL_HOME}
#echo "set session resource_pool=${RESOURCEPOOL};" > ${VSQL_HOME}/.vsqlrc

#parameters
CASENAME="$1"
if [ -z "$CASENAME" ] ; then
  CASENAME="$(basename $(dirname $0))-$(basename $0)"
else
  CASENAME="${CASENAME}-$(basename $0)"
fi

curDir=/home/dbadmin/ZJTELCOM_POC/
scriptDir=$(cd "$(dirname $0)"; pwd)

logDir=${curDir}/logs
logFile=${logDir}/TPCDS.log

mkdir -p ${logDir}
cat /dev/null > ${logFile}

echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)

vsql -d POC -U dbadmin -w dbadmin -f ${scriptDir}/4.1.6.sql >  ${logFile} 


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)

echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile} File} 
