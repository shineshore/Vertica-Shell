#!/bin/bash

echo "4.1.6 TPCDS_200G查询测试"

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
scriptDir=/home/dbadmin/ZJTELCOM_POC/sql/4.1/4.1.6/

logDir=${curDir}/logs
logFile=${logDir}/TPCDS_200G_QUERY.log

mkdir -p ${logDir}
cat /dev/null > ${logFile}

echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)

vsql -d POC -e -U dbadmin -w dbadmin -f ${scriptDir}/4.1.6_tpcds_200g.sql >  ${logFile} 


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)

echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile} 
