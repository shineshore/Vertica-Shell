#!/bin/bash  

HOSTNAME=`hostname`
NUM=0
i=0

while ((2 != 1))  
do   
	for (( j=${NUM};j <= 10 ;j++ ))
 	do	
		$VSQL -e -f ./4.5.sql &
		let "i += 1"
		echo $i
	done	
	
	sleep 3
	
	NUM=`ps -ef|grep "vsql -h"|grep -v grep |wc -l`
	
done
