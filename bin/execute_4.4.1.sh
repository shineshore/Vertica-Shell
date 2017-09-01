#!/bin/bash

#parameters

curDir=$(pwd)
scriptDir=$(pwd)/bin

logDir=${curDir}/logs
logFile=${logDir}/4.4.1.log

mkdir -p ${logDir}
cat /dev/null > ${logFile}


echo "inventory row count before remove nodes"
$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select count(*) from TPC.inventory;
EOF




echo "nodes state before remove nodes"
$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select node_name, node_state, export_address from nodes;
EOF


echo "begin at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


echo "/opt/vertica/bin/admintools -t kill_host --hosts=edatest05 " | tee -a ${logFile}
/opt/vertica/bin/admintools -t kill_host --hosts=edatest05  2>&1 <<-EOF 2>&1 | tee -a ${LOGFILE}
	y
EOF


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "inventory row count after node removed"
$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select count(*) from TPC.inventory;
EOF


echo "nodes state after node removed"
$VSQL <<-EOF 2>&1 | tee -a ${logFile}
  select node_name, node_state, export_address from nodes;
EOF


echo "end at `date`" | tee -a ${logFile}
echo "total time=${time_total} s" | tee -a ${logFile}

