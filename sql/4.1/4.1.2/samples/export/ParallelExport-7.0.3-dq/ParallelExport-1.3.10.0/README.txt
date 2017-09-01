****************************
* Vertica Analytic Database
*
* User Defined Extensions ParallelExport
*
* Copyright HP Vertica, 2013
****************************

This directory contains example code for use with Vertica User Defined  Functions (UDF) for concurrently exporting data to files or external command.  exportdata was renamed to ParalellExport.  this should not affect end users as the make file installs both the legacy exportdata function and ParallelExport (both take the same arguments).

ParallelExport is thread safe and can leverage PARTITION BEST in the over clause for faster file output.

parameters:
    If path or cmd includes ${nodeName}, this UDL will read different file, just replace ${nodeName} with current NodeName as used by vertica.
    If path or cmd includes ${hostName}, this UDL will read different file, just replace ${hostName} with current machine's hostname .
	path: data file path, must be writeable for each node Optional parameter, default value is '', means output to stdout.
	cmd: data saving cmd, eg:
		 saving data to a file: 
		 	cat - > /tmp/test.txt
		 saving data to HDFS: 
			cmd='
				(
				URL="http://v001:50070/webhdfs/v1/test.txt-`hostname`"; 
				USER="root"; 
				curl -X DELETE "$URL?op=DELETE&user.name=$USER"; 
				MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr ''\r'' ''\n''`; 
				Ck=`echo $MSG | awk -F ''Location:'' ''{print $1}'' | awk -F ''Set-Cookie:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
				Loc=`echo $MSG | awk -F ''Location:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
				curl -b "$Ck" -X PUT -T - "$Loc";
				) 2>&1 > /dev/null
				'
	buffersize: writing buffer size(bytes). Optional parameter, default value is 1024.
	separator: separator string for concatenating. Optional parameter, default value is '|'.
	fromcharset: source encoding. Optional parameter, default value is ''.
	tocharset: target encoding. Optional parameter, default value is ''.

usages:
	echo export to file on each node ...
		select exportdata(ID, Name, Descpt 
			using parameters path=:PWD||'/test/export-utf8.txt.${nodeName}'
			) over (partition auto) 
		  from exportdataTEST.NUMBERS;
    or
        select ParallelExport(ID, Name, Descpt 
			using parameters path=:PWD||'/test/export-utf8.txt.${nodeName}'
			) over (partition auto) 
		  from exportdataTEST.NUMBERS;
	
	echo export to file on each node, with gb18030 encoding ...
		select exportdata(ID, Name, Descpt 
			using parameters path=:PWD||'/test/export-gbk.txt.${nodeName}', separator=',', fromcharset='utf8', tocharset='gb18030'
			) over (partition auto) 
		  from exportdataTEST.NUMBERS;
    or
		select ParallelExport(ID, Name, Descpt 
			using parameters path=:PWD||'/test/export-gbk.txt.${nodeName}', separator=',', fromcharset='utf8', tocharset='gb18030'
			) over (partition auto) 
		  from exportdataTEST.NUMBERS;

Combine the following commands with ${nodeName} or ${hostName}, but NEVER combine a command with OVER(PARTITION BEST)
useful commands:
    cmd='bzip2 -9 -c - > <file path>.bz2' -- This will save a bzipped archive to the specified file path
    cmd='gzip -9 -c - > <file path>.gz' -- This will save a gzipped archive to the specified file path
    cmd='split -d -l 100000 - <file path>' -- This will use the file path as a prefix for files and create a number of files with 100000 records each
    
Build
  make run

Install
  make install

Uninstall
  make uninstall

Test In Database
  make run
