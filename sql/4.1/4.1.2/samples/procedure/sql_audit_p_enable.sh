#!/bin/bash

if [ $# -neq 2 ] ; then
  echo "Usage: SQL_AUDIT_ENABLE(tableName, 'I' | 'U' | 'D' | 'S')"
  exit 1
fi

tableName=$1
operatorType=$2

# Note: VSQL is a environment parameter, including vsql and username/password args
$VSQL <<-EOF>>/dev/null
  
  -- SQLs
  insert int o SQL_AUDIT_CONFIG values(upperb('$tableName'), upperb(left('$operatorType', 1)));
  
  
  commit;

EOF

exit $?
