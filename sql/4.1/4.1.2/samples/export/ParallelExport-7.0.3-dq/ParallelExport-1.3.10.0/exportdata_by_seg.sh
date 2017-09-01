#!/bin/bash

if [ $# -lt 2 ] ; then
  echo "Usage: $0 \"selectSQL (with SEGCONDITION macro in where clause ) \" dataFilename fileCountPerNode fromEncoding toEncoding"
  exit 1
fi

selectSQL="$1"
dataFilename=$2

fileCountPerNode=$3
if [ -z $fileCountPerNode ] ; then
  fileCountPerNode=1
fi

fromEncoding=$4
if [ -z $fromEncoding ] ; then
  fromEncoding="utf-8"
fi

toEncoding=$5
if [ -z $toEncoding ] ; then
  toEncoding="utf-8"
fi

# get array of node names
VSQL=`sed s/-a// <<<$VSQL`
VSQL=`sed s/-e// <<<$VSQL`
nodes=( `${VSQL} -Atq -c "select node_address from nodes where node_state='UP' order by node_address;" | tr "\\r\\n" " "` )
if [ $? -eq 0 -a ${#nodes[*]} -gt 0 ] ; then
  for (( n=0 ; n<${#nodes[*]} ; n++ )) ; do
	exp="\(\(9223372036854775807\/\/${#nodes[*]}\)\+1\)\=${n}"
	fullSQL=`sed s/SEGCONDITION/$exp/ <<<$selectSQL`

    node=${nodes[$n]}
    
    if [ $fileCountPerNode -eq 1 ] ; then
      if [ "$fromEncoding" = "$toEncoding"  ] ; then
        ssh -n $node "$VSQL -Atq -F '|' -o $dataFilename-$node -c \"$fullSQL;\" " 2>&1 &
      else
        pFilename="/tmp/`basename $dataFilename`-`date +%Y%m%d%H%M%S`-${RANDOM}.pip"
        ssh -n $node "mkfifo $pFilename; $VSQL -Atq -F '|' -o $pFilename -c \"$fullSQL;\" &  iconv -c -f $fromEncoding -t $toEncoding $pFilename > $dataFilename-$node; rm -f $pFilename" 2>&1 &
      fi
    else
      rows=`${VSQL} -Atq -c "select count(*) from ($fullSQL) t;"`
      if [ $? -eq 0 ] ; then
        # echo rows count=$rows, nodes count=${#nodes[*]}, files count per node=$fileCountPerNode
      
        inc=$(( $rows /$fileCountPerNode ))
        start=0
        nlimit=$rows
      
        # segment per node
        finc=$(( $nlimit / $fileCountPerNode ))
        fmaxpos=$(( $nlimit + $start ))
        flimit=$finc
        for (( f=1 ; f<=$fileCountPerNode ; f++ )) ; do
          if (( $f == $fileCountPerNode )) ; then
            flimit=$(( $fmaxpos - $start ))
          fi
      
          if [ "$fromEncoding" = "$toEncoding"  ] ; then
            ssh -n $node "$VSQL -Atq -F '|' -o $dataFilename-$node-$f -c \"$fullSQL limit $flimit OFFSET $start;\" " 2>&1 &
          else
            pFilename="/tmp/`basename $dataFilename`-`date +%Y%m%d%H%M%S`-${RANDOM}.pip"
            ssh -n $node "mkfifo $pFilename; $VSQL -Atq -F '|' -o $pFilename -c \"$fullSQL limit $flimit OFFSET $start;\" &  iconv -c -f $fromEncoding -t $toEncoding $pFilename > $dataFilename-$node-$f; rm -f $pFilename" 2>&1 &
          fi
      
          start=$(( $start + $flimit ))
        done
      fi
    fi
  done

  wait
fi
