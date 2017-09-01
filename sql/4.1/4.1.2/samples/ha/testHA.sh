#!/bin/bash

#parameters

CASENAME="$1"
if [ -z "${CASENAME}" ] ; then
  CASENAME="$(basename $0)"
else
  CASENAME="${CASENAME}-$(basename $0)"
fi

curDir=$(pwd)
scriptDir=$(cd "$(dirname $0)"; pwd)

logDir=${curDir}/logs
logFile=${logDir}/${CASENAME}.log

mkdir -p ${logDir}
cat /dev/null > ${logFile}


${VSQL} <<- EOF 2>&1 | tee -a ${logFile}
  drop table if exists testNodeFailingInsert cascade;
  create table testNodeFailingInsert like online_sales.online_sales_fact including projections;

EOF


echo "begin ${CASENAME} at $(date) ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


${VSQL} <<- EOF 2>&1 | tee -a ${logFile}
  select sysdate begintime;
  select count(*)  from testNodeFailingInsert;
  
  -- 让这一步执行时间长些，在执行过程中模拟节点故障，观察是否不中断继续执行，结果是否正确
  insert /*+ direct */ into testNodeFailingInsert select * from online_sales.online_sales_fact;
  commit;
  
  select sysdate endtime;
  select count(*)  from testNodeFailingInsert;

EOF


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


${VSQL} <<- EOF 2>&1 | tee -a ${logFile}
  drop table if exists testNodeFailingInsert cascade;

EOF


echo "end ${CASENAME} at $(date)" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}

