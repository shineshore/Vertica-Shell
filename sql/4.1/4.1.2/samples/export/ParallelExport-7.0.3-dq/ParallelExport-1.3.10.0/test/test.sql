/*****************************
 * Vertica Analytic Database
 *
 * exportdata User Defined Functions
 *
 * Copyright HP Vertica, 2013
 */



\o /dev/null
create schema if not exists exportdataTEST;
set search_path = public,exportdataTEST;

\set PWD '\''`pwd`'\''

create table if not exists exportdataTEST.NUMBERS(
  ID int
  , Name CHAR(20)
  , Descpt VARCHAR(20)
)
segmented by hash(ID) all nodes
;

CREATE TABLE IF NOT EXISTS exportdataTEST.all_fields(a BOOL, b DATE, c FLOAT, d INT, e INTERVAL, f INTERVALYM, g NUMERIC, h VARCHAR(200), i TIMESTAMP, j TIMESTAMPTZ, k TIME, l TIMETZ) SEGMENTED BY hash(b,c,d) ALL NODES;

truncate table exportdataTEST.NUMBERS;
truncate table exportdataTEST.all_fields;

copy exportdataTEST.NUMBERS from stdin delimiter ',' direct;
1,One,一
2,Two,二
3,Three,三
4,Four,四
5,Five,五
6,Six,六
7,Seven,七
8,Eight,八
9,Nigh,九
10,Ten,十
\.


\o

\echo export to file on each node ...
select exportdata(ID, Name, Descpt 
	using parameters path=:PWD||'/test/export-utf8.txt.${nodeName}'
	) over (partition auto) 
  from exportdataTEST.NUMBERS;


\echo export to file on each node, with gb18030 encoding ...
select exportdata(ID, Name, Descpt 
	using parameters path=:PWD||'/test/export-gbk.txt.${nodeName}', separator=',', fromcharset='utf8', tocharset='gb18030'
	) over (partition auto) 
  from exportdataTEST.NUMBERS;

\echo export to saving cmd on each node, with gb18030 encoding ...
select exportdata(ID, Name, Descpt 
	using parameters cmd='cat - > '||:PWD||'/test/test.txt', separator=',', fromcharset='utf8', tocharset='gb18030'
	) over (partition auto) 
  from exportdataTEST.NUMBERS;

--\echo export to HADOOP/HDFS with gb18030 encoding ...
--select exportdata(ID, Name, Descpt 
--	using parameters cmd='
--(
--URL="http://v001:50070/webhdfs/v1/test.txt-`hostname`"; 
--USER="root"; 
--curl -X DELETE "$URL?op=DELETE&user.name=$USER"; 
--MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr ''\r'' ''\n''`; 
--Ck=`echo $MSG | awk -F ''Location:'' ''{print $1}'' | awk -F ''Set-Cookie:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
--Loc=`echo $MSG | awk -F ''Location:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
--curl -b "$Ck" -X PUT -T - "$Loc";
--) 2>&1 > /dev/null
--	', separator=',', fromcharset='utf8', tocharset='gb18030'
--	) over (partition auto) 
--  from exportdataTEST.NUMBERS;

\set source '\''`pwd`'/test/input\''
\set destination '\''`pwd`'/test/output\''
\echo Verifying Output Format
-- CREATE OR REPLACE TABLE exportdataTEST.all_fields(a BOOL, b DATE, c FLOAT, d INT, e INTERVAL, f INTERVALYM, g NUMERIC, h VARCHAR(200), i TIMESTAMP, j TIMESTAMPTZ, k TIME, l TIMETZ);
COPY exportdataTEST.all_fields FROM :source ABORT ON ERROR;
SELECT ParallelExport(a,b,c,d,e,f,g,h,i,k,l,j USING PARAMETERS path=:destination) OVER (PARTITION AUTO) FROM exportdataTEST.all_fields;
-- SELECT ParallelExport(j USING PARAMETERS path=:destination) OVER (PARTITION AUTO) FROM exportdataTEST.all_fields WHERE (MONTH(j) > 3 AND MONTH(j) < 11) OR (MONTH(j) = 11 and DAY(j) < 2) OR (MONTH(j) = 3 AND DAY(j) > 9);

-- DROP SCHEMA exportdataTEST CASCADE;
