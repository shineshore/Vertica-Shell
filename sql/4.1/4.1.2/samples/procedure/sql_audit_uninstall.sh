#!/bin/bash

#
# uninstall sql audit package
#

if [ $# -lt 1 ] ; then
  echo Usage: $0 DB_NAME [DB_PWD]
  exit 
fi

DB_NAME=$1

vsqlDB_PWD=
adminDB_PWD=
if [ $# -gt 1 ] ; then
  vsqlDB_PWD="-w $2"
  adminDB_PWD="-p $2"
fi


# uninstall procedures 
scriptDir=$(cd "$(dirname $0)"; pwd)

/opt/vertica/bin/vsql ${DB_NAME} -U dbadmin ${vsqlDB_PWD} <<-EOF
  DROP PROCEDURE SQL_AUDIT_ENABLE(tableName varchar, operator varchar);
EOF

