#!/bin/bash

#parameters

# set resource pool
RESOURCEPOOL="adhoc_query_pool"
export VSQL_HOME=/tmp/pools/$(basename $(dirname $0))_${RESOURCEPOOL}
mkdir -p ${VSQL_HOME}
echo "set session resource_pool=${RESOURCEPOOL};" > ${VSQL_HOME}/.vsqlrc

CASENAME="$1"
if [ -z "$CASENAME" ] ; then
  CASENAME="$(basename $(dirname $0))-$(basename $0)"
else
  CASENAME="${CASENAME}-$(basename $0)"
fi


curDir=$(pwd)
scriptDir=$(cd "$(dirname $0)"; pwd)

logDir=${curDir}/logs
logFile=${logDir}/${CASENAME}.log

mkdir -p ${logDir}
cat /dev/null > ${logFile}



echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


$VSQL <<-EOF 2>&1 |tee -a ${logFile}
  \timing
  
  SELECT /*+ label(3_3_4_8) */ CSMI_TYPE,
         COUNT(DISTINCT serv_number)
    FROM TB_DM_CT_CMSI_USER_SERVICE_MON
   GROUP BY CSMI_TYPE
   ORDER BY 1, 2;

EOF


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}

