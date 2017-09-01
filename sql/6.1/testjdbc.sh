CURRENT_PATH=`pwd`

javac JDBCUtlTool.java

export CLASSPATH="$CLASSPATH:$CURRENT_PATH/vertica-jdbc.jar"

java JDBCUtlTool

