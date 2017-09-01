#!/bin/bash

#parameters
COLUMNS=20

DBNAME="$1"
if [ -z "$DBNAME" ] ; then
  DBNAME=$(ps -ef | grep 'bin/vertica -D' | grep -v grep | awk '{print $12}' | head -1)
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


${VSQL} <<- EOF 2>&1 | tee -a ${logFile}
  drop resource pool test_insert_pool;
  create resource pool test_insert_pool 
    priority 100
    runtimepriority HIGH
    runtimeprioritythreshold 0
    executionparallelism 1
    plannedconcurrency 9
    maxconcurrency 8
    queuetimeout NONE;

  
  drop table if exists test_insert;
  create table if not exists test_insert(
    $(
	for((n=1; n<=COLUMNS; n++)) ; do
      printf "c${n} "
	  if((n % 2 == 1)) ; then
	    printf "int"
	  else
		printf "varchar(8)"
	  fi
	  if((n < COLUMNS)) ; then
		printf ","
	  fi
	  printf "\n"
    done
    )
  )
  order by c1
  segmented by hash(c1) all nodes;

EOF


test_insert()
{
	#parameters
	JOBS="$1"
	CONCURRENCY="$2"
	if [ -z "$JOBS"  -o -z "$CONCURRENCY" ] ; then
	  echo "Usage: $0 JOBS CONCURRENCY"
	  exit 1
	fi

	time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)

	
	tmpscriptFile="/tmp/$(basename $0)-$(date +%s%N)-${RANDOM}.java"; cat  <<-EOF > ${tmpscriptFile} ; cd ${scriptDir}; java -cp "$(echo ${scriptDir}/*.jar | tr ' ' ':'):/opt/vertica/java/lib/vertica-jdbc.jar" bsh.Interpreter ${tmpscriptFile} > ${logFile}; cd ${curDir}; rm -f ${tmpscriptFile}
	
	  import java.io.*;
	  import java.util.*;
	  import java.sql.*;
	  
	  public class Test extends Thread {
	      public void run() {
	        Statement stmt = null;
	      	try{
			  stmt = this.conn.createStatement();
	          for(int i=this.iStart; i<this.iStart+this.iCount; i++){
				StringBuilder query = new StringBuilder("insert into test_insert values(");
				for(n=1; n<=${COLUMNS}; n++) {
				  if(n % 2 == 1) {
					query.append(i);
				  }
				  else{
					query.append("'").append(i).append("'");
				  }
				  if(n < ${COLUMNS}) {
					query.append(",");
				  }
				}
				query.append(");");
				stmt.executeUpdate(query.toString());
				System.out.println("finish query(returncount=1)");
	          }
	        }
	        catch (SQLException e) {
	          e.printStackTrace();
	        }
			finally{
			  if (stmt != null)
				stmt.close();
			  this.conn.commit();
	          this.conn.close();
	        }
	      }
	      
	      public Test(Connection conn, int iStart, int iCount) {
	        this.conn = conn;
	        this.iStart = iStart;
	        this.iCount = iCount;
	      }
	      
	      Connection conn;
	      int iStart;
	      int iCount;
	  }
	
	
	  // main
	  int JOBS=${JOBS};
	  int CONCURRENT=${CONCURRENCY};
	  try {
	      // initialize connection pool
		  Class.forName("com.vertica.jdbc.Driver"); // not need since Java 6
		  String connectionString = "jdbc:vertica://localhost:5433/${DBNAME}?user=dbadmin&password=&&autocommit=false&loglevel=none";
		  Properties myProp = new Properties();
		  myProp.put("ConnSettings", "set session resource_pool=test_insert_pool");
	
	      // launch
	      long start = System.currentTimeMillis();
	      List threads = new ArrayList(CONCURRENT);
	      int countPerScript = JOBS/CONCURRENT;
	      for (int i=0; i < CONCURRENT; i++) {
			int iStart = countPerScript*i;
	        if(i == CONCURRENT-1) {
	          countPerScript = JOBS - iStart;
	        }
		    Connection conn = DriverManager.getConnection(connectionString, myProp);
	        Test t = new Test(conn, iStart, countPerScript);
	        threads.add(t);
	        t.start();
	      }
	  
	      // wait for complete
	      Iterator it = threads.iterator();
	      while (it.hasNext()) {
	        Test t = (Test)it.next();
	        try {
	          t.join();
	        } catch (InterruptedException e) {}
	      }
	      long elapsed = System.currentTimeMillis() - start;
	  
	      // destroy connection pool
	  } catch (SQLException e) {
	      e.printStackTrace();
	  }
	  
	  exit();
	EOF

	time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
	time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)
	
	# caculate QPS
	QueryCount=$(grep "finish query" ${logFile} | wc -l)
	Time=${time_total}
	QPS=$(echo "scale=3; ${QueryCount}/${Time}" | bc)
	echo "${CASENAME} concurrency=${CONCURRENCY} finished queries count=${QueryCount} in time=${Time} s, QPS=${QPS}" | tee -a ${logFile}
}


echo "begin ${CASENAME} at $(date) ..." | tee -a ${logFile}
time_begin=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)


test_insert 100 1

#sleep 2
test_insert 200 2

#sleep 2
test_insert 400 4

#sleep 2
test_insert 800 8


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "end ${CASENAME} at $(date)" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}

