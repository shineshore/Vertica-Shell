#!/bin/bash

CurDir=$(pwd)
ScriptDir=$(cd $(dirname $0); pwd)

# get array of node names
VSQL=`sed s/-a// <<<$VSQL`
VSQL=`sed s/-e// <<<$VSQL`
tables=$($VSQL -F. -Atc "select quote_ident(table_schema) from tables;")
for t in $tables ; do
  echo tablename: t;
done
