#!/bin/bash

#parameters
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


echo "data on each node before add nodes"
$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select node_name, sum(used_bytes)//1024//1024//1024 as used_gigabytes
     from projection_storage
     group by node_name;
EOF


echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


echo "/opt/vertica/bin/admintools -t db_add_node --hosts=v004,v005 --database=VMart --noprompts" | tee -a ${logFile}
/opt/vertica/bin/admintools -t db_add_node --hosts=v004,v005 --database=VMart --password vertica --noprompts 2>&1 | tee -a ${logFile}


$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  \timing
  
  select rebalance_cluster();
EOF


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select SET_SCALING_FACTOR(3);
EOF

echo "data on each node after adding nodes"
$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select node_name, sum(used_bytes)//1024//1024//1024 as used_gigabytes
     from projection_storage
     group by node_name;
EOF


echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}

