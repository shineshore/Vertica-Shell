#!/bin/bash

#parameters

DBNAME="$1"
if [ -z "$DBNAME" ] ; then
  DBNAME=$(ps -ef | grep 'bin/vertica -D' | grep -v grep | awk '{print $12}' | head -1)
else
  shift
fi

CASENAME="$1"
if [ -z "${CASENAME}" ] ; then
  CASENAME="$(basename $0)"
else
  CASENAME="${CASENAME}-$(basename $0)"
fi

curDir=$(pwd)
scriptDir=$(cd "$(dirname $0)"; pwd)

logDir=${curDir}/logs
logFile=${logDir}/${CASENAME}.log

mkdir -p ${logDir}
cat /dev/null > ${logFile}

test_query()
{
	time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)

	tmpscriptFile="/tmp/$(basename $0)-$(date +%s%N)-${RANDOM}.java"; cat  <<-EOF > ${tmpscriptFile} ; cd ${scriptDir}; java -cp "$(echo ${scriptDir}/utils/*.jar | tr ' ' ':'):/opt/vertica/java/lib/vertica-jdbc.jar" bsh.Interpreter ${tmpscriptFile} | tee ${logFile}; cd ${curDir}; rm -f ${tmpscriptFile}
	
	  import java.io.*;
	  import java.util.*;
	  import java.util.logging.*;
	  import java.sql.*;
	  import javassist.util.proxy.*;
	  import javassist.*;
	  import com.mchange.v2.c3p0.*;
	  
	  import com.vertica.jdbc.*;
	  import com.vertica.jdbc.common.*;
	  import com.vertica.core.*;
	  import com.vertica.jdbc.hybrid.*;

	
	  // main
	  try {
	      // initialize connection
	      String strDriver = "com.vertica.jdbc.Driver";
		  Class.forName(strDriver); // not need since Java 6
		  String connectionString = "jdbc:vertica://localhost:5433/${DBNAME}?user=dbadmin&password=&autocommit=false&loglevel=none";
		  Properties myProp = new Properties();
		  //myProp.put("ConnSettings", "set session resource_pool=general;");

		  Logger.getLogger("com.mchange").setLevel(Level.WARNING);
		  ComboPooledDataSource datasource = new ComboPooledDataSource();
		  datasource.setDriverClass(strDriver);
		  datasource.setJdbcUrl(connectionString);
		  datasource.setProperties(myProp);
	
	      String query = "select * from nodes;";
	      System.out.println("start query: " + query);
	      Connection conn = null;
	      Statement stmt = null;
	      try {
	        conn = datasource.getConnection();
	        stmt = conn.createStatement();
	        ResultSet rs = stmt.executeQuery(query);
	        while (rs.next()) {
			  StringBuilder sbLine = new StringBuilder();
	          int numberOfColumns = rs.getMetaData().getColumnCount();
	          for(int c=1; c<=numberOfColumns; c++) {
	            if(c>1){
	              sbLine.append("|");
	            }
	            String v = rs.getString(c);
	            sbLine.append( (v==null)? "": v );
	          }
			  System.out.println(sbLine.toString());
	        }
	      }
	      catch (SQLException e) {
	        e.printStackTrace();
	      }
	      finally{
	        stmt.close();
	        conn.close();
	      }
	  
	      // destroy connection
	      datasource.close();
	  } catch (SQLException e) {
	      e.printStackTrace();
	  }
	  
	  exit();
	EOF

	time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
	time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)
}


test_query 

