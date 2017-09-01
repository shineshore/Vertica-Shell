#!/bin/sh

#parameters
CASENAME="$1"
if [ -z "$CASENAME" ] ; then
  CASENAME="$(basename $0)"
fi

curDir=$(pwd)
scriptDir=$(cd "$(dirname $0)"; pwd)

logDir=${curDir}/logs
logFile=${logDir}/${CASENAME}.log

mkdir -p $logDir
cat /dev/null > $logFile


echo "begin ${CASENAME} at `date` ..." | tee -a $logFile
time_begin=$(date +%s )


VSQL_F=$(sed s/-a// <<<$VSQL); VSQL_F=$(sed s/-e// <<<$VSQL_F)
tables=( $($VSQL_F -Atq -c "select upper(table_schema||'.'||table_name) from tables where table_name like 'OFR_MAIN_ASSET_CUR_A_01%' or table_name like 'CATALOG_SALES_%' order by 1;"  2>&1 | grep -vw "WARNING" | tr "\\r\\n" " ") )
if [ $? -eq 0 -a ${#tables[*]} -gt 0 ] ; then
  printf "TABLE_NAME\tROW_COUNT\tUSED_BYTES\n" | tee -a $logFile
  for (( n=0 ; n<${#tables[*]} ; n++ )) ; do
    tableName=${tables[$n]}
    
    count=$($VSQL_F -Atqc "select count(*) from $tableName" 2>&1 | grep -vw "WARNING")
    size=$($VSQL_F -Atqc "select sum(used_bytes) from projection_storage where upper(anchor_table_schema||'.'||anchor_table_name)='$tableName'" 2>&1 | grep -vw "WARNING")
    if [ -z "$size" ] ; then
      size=0
    fi
    printf "%s\t%s\t%s\n" $tableName $count $size | tee -a $logFile
  done
fi


time_end=$(date +%s )
time_total=`expr ${time_end} - ${time_begin}`

echo "end ${CASENAME} at `date`" | tee -a $logFile
echo "${CASENAME} total time=${time_total} s" | tee -a $logFile
