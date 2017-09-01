#!/bin/bash

#parameters
CASENAME="$(basename $0)"

curDir=$(pwd)
scriptDir=$(cd "$(dirname $0)"; pwd)

logDir=${curDir}/logs
logFile=${logDir}/${CASENAME}.log

mkdir -p $logDir
cat /dev/null > $logFile


echo "begin ${CASENAME} at `date` ..." | tee -a $logFile
time_begin=$(date +%s )

# batch in sequence:
$VSQL -v ON_ERROR_STOP=1 <<-EOF 2>&1 >> $logFile
  \timing
  
EOF
rc=$?
echo DEBUG: rc=$rc
if [ $rc != 0 ] ; then
  exit 2001
fi


# batch in parallel:

# begin parallel jobs
A_JOBIDS=()

$VSQL -v ON_ERROR_STOP=1 <<-EOF 2>&1 >> $logFile &
  \timing
  
EOF
A_JOBIDS[${#A_JOBIDS[@]}]=$!

$VSQL -v ON_ERROR_STOP=1 <<-EOF 2>&1 >> $logFile &
  \timing
  
EOF
A_JOBIDS[${#A_JOBIDS[@]}]=$!

# end parallel jobs: wait and check error
rc=0
echo jobs count=${#A_JOBIDS[*]}
if [ $? -eq 0 -a ${#A_JOBIDS[*]} -gt 0 ] ; then
  for (( n=0 ; n<${#A_JOBIDS[*]} ; n++ )) ; do
    wait ${A_JOBIDS[$n]}
    let "rc=$rc | $?"
  done
fi
echo DEBUG: rc=$rc
if [ $rc != 0 ] ; then
  exit 2001
fi

# batch in sequence:
$VSQL -v ON_ERROR_STOP=1 <<-EOF 2>&1 >> $logFile
  \timing
  
EOF
rc=$?
echo DEBUG: rc=$rc
if [ $rc != 0 ] ; then
  exit 2001
fi


time_end=$(date +%s )
time_total=`expr ${time_end} - ${time_begin}`

echo "end ${CASENAME} at `date`" | tee -a $logFile
echo "${CASENAME} total time=${time_total} s" | tee -a $logFile
