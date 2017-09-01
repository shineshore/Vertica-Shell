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

echo "begin ${CASENAME} at `date` ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)

#vbr.py --task listbackup --config-file ${scriptDir}/Full_Backup_NEEQ.ini
#vbr.py --task restore --archive 20160422_021011 --config-file ${scriptDir}/Full_Backup_NEEQ.ini | tee -a ${logFile}
vbr.py --task restore --config-file ${scriptDir}/vbr.ini | tee -a ${logFile}

time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)

echo "end ${CASENAME} at `date`" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}
