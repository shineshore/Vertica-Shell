#!/bin/bash

#
# install sql audit package
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


# install procedures
scriptDir=$(cd "$(dirname $0)"; pwd)

chown dbadmin:verticadba $scriptDir/sql_audit_p_enable.sh
chmod +s $scriptDir/sql_audit_p_enable.sh
admintools -t install_procedure -d $DB_NAME -f $scriptDir/sql_audit_p_enable.sh $adminDB_PWD

/opt/vertica/bin/vsql ${DB_NAME} -U dbadmin ${vsqlDB_PWD} <<-EOF
  CREATE PROCEDURE SQL_AUDIT_ENABLE(tableName varchar, operator varchar) AS 'sql_audit_p_enable.sh' LANGUAGE 'external' USER 'dbadmin';
  grant all on procedure SQL_AUDIT_ENABLE(tableName varchar, operator varchar) to dbadmin;
EOF
