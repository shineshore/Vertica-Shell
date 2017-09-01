#!/bin/bash

#parameters
TARGETPATH=/data/export
CONCURRENCY=1

# resource pool
RESOURCEPOOL="exporting_pool"
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

${VSQL} -X <<- EOF 2>&1 | tee -a ${logFile}
  drop resource pool exporting_pool;
  create resource pool exporting_pool 
	plannedconcurrency $((CONCURRENCY+1))
    maxconcurrency ${CONCURRENCY}
    queuetimeout NONE;

EOF

echo "begin ${CASENAME} at $(date) ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


echo "exporting table online_sales.online_sales_fact" | tee -a ${logFile}

for (( n=0 ; n<${CONCURRENCY} ; n++ )) ; do
	$VSQL <<-EOF 2>&1 | tee -a $logFile &
	  \timing
	
	  select ParallelExport(* 
	    using parameters path='${TARGETPATH}/online_sales.online_sales_fact.dat.\${nodeName}-${n}'
	  	  , separator='|'
	  	) over (partition auto) 
	    from online_sales.online_sales_fact 
		where hash(online_page_key)%${CONCURRENCY} = ${n} 
	    ;
	EOF
done
wait


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "end ${CASENAME} at $(date)" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}


