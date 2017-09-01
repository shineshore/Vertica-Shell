#!/bin/bash

#parameters
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
  drop resource pool testkv_query_pool;
  create resource pool testkv_query_pool 
    priority 100
    runtimepriority HIGH
    runtimeprioritythreshold 0
    executionparallelism 1
    plannedconcurrency 10
    maxconcurrency 9
    queuetimeout NONE;
EOF

PAMFILE=${logFile}.pam
VSQL_F=`sed s/-a// <<<$VSQL`;VSQL_F=`sed s/-e// <<<$VSQL_F`
${VSQL_F} -XAqtc "select distinct online_page_key from online_sales.online_sales_fact order by 1 limit 100000" > ${PAMFILE}

test_query()
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
			  stmt = conn.createStatement();
	          for(int i=0; i<this.iCount; i++){
	            Long id =  Long.parseLong( arrIDs.get( (int)(arrIDs.size() * Math.random()) ) );
	            String query = "select count(*) from online_sales.online_sales_fact where online_page_key = " + id.toString();
	            try {
	              // output result
	              StringBuilder sbLine = new StringBuilder();
				  ResultSet rs = stmt.executeQuery(query);
				  int rowCount = 0;
	              while (rs.next()) {
	                int numberOfColumns = rs.getMetaData().getColumnCount();
	                for(int c=1; c<=numberOfColumns; c++) {
	                  if(c>1){
	                    sbLine.append("|");
	                  }
	                  sbLine.append(rs.getString(c));
	                }
					rowCount++;
	              }
	              sbLine.append("\nfinish query").append("(returncount=").append(rowCount).append("): ");
	              System.out.println(sbLine.toString());
	            }
	            catch (SQLException e) {
	              e.printStackTrace();
	            }
	          }
	        }
	        catch (SQLException e) {
	          e.printStackTrace();
	        }
			finally{
			  if (stmt != null)
				stmt.close();
	          conn.close();
	        }
	      }
	      
	      public Test(Connection conn, ArrayList arrIDs, int iCount) {
	        this.conn = conn;
	        this.arrIDs = arrIDs;
	        this.iCount = iCount;
	      }
	      
	      Connection conn;
	      ArrayList arrIDs;
	      int iStart;
	      int iCount;
	  }
	
	
	  // main
	  int JOBS=${JOBS};
	  int CONCURRENT=${CONCURRENCY};
	  try {
	      // read parameters from a file
	      ArrayList arrIDs = new ArrayList(JOBS);
	      File inFile = new File("${PAMFILE}");
	      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inFile)));
	      
	      String id;
	      while (((id = br.readLine()) != null)) {
	        arrIDs.add(id);
	      }
	      br.close();
	      
	      // initialize connection pool
		  Class.forName("com.vertica.jdbc.Driver"); // not need since Java 6
		  String connectionString = "jdbc:vertica://localhost:5433/${DBNAME}?user=dbadmin&password=&&autocommit=false&loglevel=none";
		  Properties myProp = new Properties();
		  myProp.put("ConnSettings", "set session resource_pool=testkv_query_pool");
	
	      // launch
	      long start = System.currentTimeMillis();
	      List threads = new ArrayList(CONCURRENT);
	      for (int i=0; i < CONCURRENT; i++) {
	        int countPerScript = JOBS/CONCURRENT;
	        if(i == CONCURRENT-1) {
	          countPerScript = JOBS - countPerScript*i;
	        }
		    Connection conn = DriverManager.getConnection(connectionString, myProp);
	        Test t = new Test(conn, arrIDs, countPerScript);
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


test_query 100 1

sleep 2
test_query 1000 10

sleep 2
test_query 2000 20

sleep 2
test_query 3000 30


time_end=$(echo "$(date +%s)*1000 + $(date +%N)/1000000" | bc)
time_total=$(echo "scale=3;  (${time_end}-${time_begin})/1000" | bc)


echo "end ${CASENAME} at $(date)" | tee -a ${logFile}
echo "${CASENAME} total time=${time_total} s" | tee -a ${logFile}


#rm -rf ${logFile}
rm -rf ${PAMFILE}
