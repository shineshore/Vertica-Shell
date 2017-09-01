#!/bin/bash

CurDir=$(pwd)
ScriptDir=$(cd $(dirname $0); pwd)

ResDir=./log
LogFile=$ResDir/$0.log

mkdir -p $ResDir
cat /dev/null > $LogFile


echo begin loading data at `date` | tee -a $LogFile

$VSQL <<-EOF 2>&1 | tee -a $LogFile
	\timing
	
	select sysdate;
EOF

echo end generating data at `date` | tee -a $LogFile
