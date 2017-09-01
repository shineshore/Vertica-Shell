#!/bin/bash

CurDir=$(pwd)
ScriptDir=$(cd $(dirname $0); pwd)

# get array of node names
VSQL=`sed s/-a// <<<$VSQL`
VSQL=`sed s/-e// <<<$VSQL`
nodes=( `$VSQL -Atq -c "select node_address from nodes where node_state='UP' order by node_address;" | tr "\\r\\n" " "` )
if [ $? -eq 0 -a ${#nodes[*]} -gt 0 ] ; then
  for (( n=0 ; n<${#nodes[*]} ; n++ )) ; do
    node=${nodes[$n]}
    echo node[$n]=${nodes[$n]}
  done
fi
