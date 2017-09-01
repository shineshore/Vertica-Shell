#!/bin/bash

CurDir=$(cd "$(dirname $0)"; pwd)

$VSQL <<-EOF
	create schema if not exists export2fileTEST;
	create table if not exists export2fileTEST.NUMBERS(
	  ID int
	  , Name CHAR(20)
	  , Descpt VARCHAR(20)
	)
	segmented by hash(ID) all nodes
	;
	
	truncate table export2fileTEST.NUMBERS;
	
	copy export2fileTEST.NUMBERS from stdin delimiter ',' direct;
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

EOF

rm -f $CurDir/test/export-*

EXPORTDATACMD=$CurDir/exportdata_by_seg.sh
$EXPORTDATACMD "select * from export2fileTEST.NUMBERS where hash(ID)//SEGCONDITION" $CurDir/test/export-utf8.txt 2 utf-8 utf-8
$EXPORTDATACMD "select * from export2fileTEST.NUMBERS where hash(ID)//SEGCONDITION" $CurDir/test/export-gbk.txt 2 utf-8 gb18030


$VSQL <<-EOF
	drop schema if exists export2fileTEST cascade;
EOF
