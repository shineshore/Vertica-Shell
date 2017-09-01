#! /opt/vertica/oss/python/bin/python

#Copyright (c) 2006 - 2014, Hewlett-Packard Development Co., L.P. 
# 
# Description: 
# vBuddyLite: allow user to run sql queries to check their cluster and help troubleshooting
# vBuddyLite is a lighter version of vBuddy, developed by the CSE team in Cambridge, US.
# It can run on any vertica node.
#
# vBuddyLite is based on adminController used for adminTools. The following classes were
# borrowed from adminTools and altered for vBuddyLite:
#
# adminController
# Navigation
# adminMeta
# uiMgr
# Menu
#
#
# Original Author: Ifigeneia Derekli, Iain Henderson
# Query Review: Po Hong
# Query Contributors: Po Hong, Maurizio Felici, Igor Marchenko, Quan Li
# Hardware Tests Contributors: Alex Jackson, Igor Marchenko
# Create Date: April 6, 2015
# Last Updated: April 12, 2017

import os
import pwd
import re
import sys
import signal
import socket
import traceback
import time
import math
import threading
from optparse import OptionParser
from vertica.utils import dialog
from vertica.tools import DBfunctions
from vertica.ui.uiMgr import *
from vertica.config import DBinclude, FileLocker, DBname
from vertica.engine import adminExec
from vertica.network import SSH, adapterpool, vsql
from decimal import Decimal
import vertica.shared.logging
vertica.shared.logging.setup_custom_logging(context='vBuddyLite')
sys.path.append("/opt/vertica/bin")

vBuddyLiteVersion = "Release 1.6.2"

#List of menus and queries for vBuddyLite. Possible syntax options:
#
#<menu_name>|<query>
#<menu_name>|<description>##<query>
#<menu_name>|<description1>##<query1>###<description2>##<query2>###<description3>##<query3>...
#<menu_name>|\! <linux_cmd>
#
#The HARDWARE TESTS menu and the GRAPHS menu are treated specially.
sqlQueriesFile = """
[1.Overview]
Cluster Overview|== VERTICA VERSION ==##SELECT /*+label(vBuddyLite)*/ version();###== LICENSE STATUS ==##SELECT /*+label(vBuddyLite)*/ get_compliance_status();###== NODE SUMMARY ==##SELECT /*+label(vBuddyLite)*/ * FROM (SELECT COUNT(*) AS number_of_nodes, SUM(CASE WHEN node_state = 'UP' THEN 1 ELSE 0 END) AS nodes_up, SUM(CASE WHEN node_state = 'DOWN' THEN 1 ELSE 0 END) AS nodes_down, SUM(CASE WHEN is_ephemeral THEN 1 ELSE 0 END) AS num_of_ephemeral_nodes  FROM nodes) AS a, (SELECT COUNT(*) AS num_of_critical_nodes FROM v_monitor.critical_nodes) AS b;###== DISK / MEMORY SUMMARY (LAST 24 HOURS) ==## SELECT /*+label(vBuddyLite)*/ c.node_name, c.min_disk_space_free_percent, c.max_disk_space_free_percent, d.min_memory_usage_percent, d.max_memory_usage_percent, e.total_memory_GB FROM   (SELECT node_name, Min(disk_space_free_percent) AS min_disk_space_free_percent,               Max(disk_space_free_percent) AS max_disk_space_free_percent        FROM   disk_storage         WHERE  storage_usage LIKE '%DATA%'        group by node_name) AS c,        (SELECT node_name, cast(Min(average_memory_usage_percent) as decimal(2,0)) || '%' AS min_memory_usage_percent,               cast(Max(average_memory_usage_percent) as decimal(2,0)) || '%' AS max_memory_usage_percent        FROM   memory_usage         WHERE  end_time > Now() - Cast('1 day' AS INTERVAL)        group by node_name        order by node_name) AS d,       (SELECT To_char(Max(total_memory) / 1024^3, '999,990.99') AS               total_memory_GB         FROM   dc_memory_info) AS e        where c.node_name = d.node_name        order by c.node_name; ###== HOST RESOURCES ==## ##Query used to identify nodes with odd resource characteristics. Ideally, all hosts have the same resources and only 1 row is returned. The first column 'num_of_hosts' is the number of nodes with the specific characteristics.##SELECT /*+label(vBuddyLite)*/ COUNT(*) AS num_of_hosts, open_files_limit, threads_limit, core_file_limit_max_size_bytes, processor_count, processor_core_count, processor_description, total_memory_bytes, total_swap_memory_bytes, disk_space_total_mb FROM host_resources GROUP BY open_files_limit, threads_limit, core_file_limit_max_size_bytes, processor_count, processor_core_count, processor_description, total_memory_bytes, total_swap_memory_bytes, disk_space_total_mb;###== SNAPSHOTS ==##SELECT /*+label(vBuddyLite)*/ * from database_snapshots;
Database Overview|== EPOCHS ==##SELECT /*+label(vBuddyLite)*/ current_epoch,ahm_epoch,last_good_epoch,refresh_epoch,designed_fault_tolerance,current_fault_tolerance FROM system;###== WOS/ROS DATA ==##SELECT /*+label(vBuddyLite)*/ TO_CHAR(wos_used_bytes/(1024^3), '9,999,990.99') AS WOS_used_GB, TO_CHAR(wos_row_count,'999,999,999,999,999') AS WOS_row_count, TO_CHAR(ros_used_bytes/(1024^3), '9,999,990.99') AS ROS_used_GB, TO_CHAR(ros_row_count,'999,999,999,999,999') AS ROS_row_count, TO_CHAR(total_used_bytes/(1024^3), '9,999,990.99') AS total_used_GB, TO_CHAR(total_row_count*1.0,'999,999,999,999,999') AS total_row_count FROM system;###== DATABASE COMPRESSION RATIO ==## With n_numbers as (  select count(*) as nodenum from nodes  )  SELECT /*+label(vBuddyLite)*/ license_name, audit_start_timestamp,  database_size_bytes AS raw_size,  compressed_size_per_storage,   Cast (database_size_bytes / compressed_size_per_storage AS DECIMAL(14, 2)) AS ratio_per_storage,  compressed_size_per_data,    Cast (database_size_bytes / compressed_size_per_data AS DECIMAL(14, 2)) AS ratio_per_data  FROM   (  SELECT license_name, database_size_bytes, audit_start_timestamp  FROM  (     SELECT       database_size_bytes, license_name, audit_start_timestamp,       ROW_NUMBER() OVER (PARTITION BY license_name ORDER BY audit_start_timestamp desc) as rn     FROM       vs_license_audits     WHERE license_name <> 'Total'  ) tb  WHERE     tb.rn = 1         ) AS A,         (  select floor(sum(compressed_size_with_HA)) as compressed_size_per_storage,         floor(sum(compressed_size_no_HA)) as compressed_size_per_data,         table_type  from (  select compressed_size_with_HA,   CASE         WHEN is_segmented = true THEN compressed_size_with_HA / 2        ELSE compressed_size_with_HA / (select nodenum from n_numbers limit 1)  END as compressed_size_no_HA,  table_type  from (  select sum(ros_used_bytes) as compressed_size_with_HA, table_type, projection_name, is_segmented  from (  select column_storage.node_name, column_name, ros_used_bytes, column_storage.anchor_table_name,      CASE         WHEN tables.is_flextable = true and column_storage.column_name = '__raw__' THEN 'Flex'         ELSE 'Regular'      END as table_type,     column_storage.projection_name,     column_storage.projection_id,     projections.is_segmented  from column_storage JOIN tables          on column_storage.anchor_table_id = tables.table_id          JOIN projections          on column_storage.projection_id = projections.projection_id) t  group by t.table_type, projection_name, is_segmented  order by t.table_type, projection_name, is_segmented  ) p1  order by table_type  ) p2  group by table_type         ) AS B         where A.license_name = B.table_type;  ###== VIEWS AND TABLES ==##SELECT /*+label(vBuddyLite)*/ (SELECT COUNT(*) FROM VIEWS) AS views_count,(SELECT COUNT(*) FROM tables) AS tables_count,( SELECT COUNT(*) FROM schemata WHERE is_system_schema = 'false') AS schemas_count, (SELECT COUNT(*) FROM projections WHERE is_segmented = 'true') AS seg_proj_count_incl_buddies,(SELECT COUNT(DISTINCT projection_schema ||'.'||projection_basename) FROM projections WHERE is_segmented = 'false') AS replicated_proj_count,(SELECT COUNT(*) FROM projections WHERE is_segmented = 'false') AS replicated_proj_count_incl_replicas;
Performance Overview (With respect to busiest hour)|== Busiest in terms of NICE% ==##  with A as ( select node_name, timestamp_trunc(start_time, 'HH') AS t_start_time, timestamp_trunc(end_time, 'HH') AS t_end_time, (nice_microseconds_end_value - nice_microseconds_start_value) AS nice_cpu_time, (system_microseconds_end_value - system_microseconds_start_value) AS system_cpu_time, (io_wait_microseconds_end_value - io_wait_microseconds_start_value) AS io_wait_cpu_time, (user_microseconds_end_value + nice_microseconds_end_value + system_microseconds_end_value + idle_microseconds_end_value + io_wait_microseconds_end_value + irq_microseconds_end_value + soft_irq_microseconds_end_value + steal_microseconds_end_value + guest_microseconds_end_value - user_microseconds_start_value - nice_microseconds_start_value - system_microseconds_start_value - idle_microseconds_start_value - io_wait_microseconds_start_value - irq_microseconds_start_value - soft_irq_microseconds_start_value - steal_microseconds_start_value - guest_microseconds_start_value)//(1000*1000) AS total_cpu_time from v_internal.dc_cpu_aggregate_by_hour WHERE  start_time > (now() - cast('25 hours' AS interval)) ), B as ( select A.node_name, A.t_start_time, A.t_end_time, sum(A.nice_cpu_time) AS t_nice_cpu_time, sum(A.system_cpu_time) AS t_system_cpu_time, sum(A.io_wait_cpu_time) AS t_io_wait_cpu_time, sum(A.total_cpu_time) AS t_total_cpu_time from A group by A.t_start_time, A.t_end_time, A.node_name order by A.t_start_time, A.t_end_time, A.node_name asc ) SELECT /*+label(vBuddyLite)*/ b1.node_name, b1.t_start_time, b1.t_end_time, cast(b1.t_nice_cpu_time/(1000*1000*3600) as decimal(4,2)) as nice_cpu_percent, cast(b1.t_system_cpu_time/(1000*1000*3600) as decimal(4,2)) as system_cpu_percent, cast(b1.t_io_wait_cpu_time/(1000*1000*3600) as decimal(4,2)) as io_wait_cpu_percent FROM B b1 LEFT OUTER JOIN B b2     ON b1.node_name = b2.node_name and b1.t_nice_cpu_time < b2.t_nice_cpu_time WHERE b2.t_nice_cpu_time IS NULL order by b1.node_name, b1.t_start_time ; ###== Busiest in terms of SYSTEM% ==##  with A as ( select node_name, timestamp_trunc(start_time, 'HH') AS t_start_time, timestamp_trunc(end_time, 'HH') AS t_end_time, (nice_microseconds_end_value - nice_microseconds_start_value) AS nice_cpu_time, (system_microseconds_end_value - system_microseconds_start_value) AS system_cpu_time, (io_wait_microseconds_end_value - io_wait_microseconds_start_value) AS io_wait_cpu_time, (user_microseconds_end_value + nice_microseconds_end_value + system_microseconds_end_value + idle_microseconds_end_value + io_wait_microseconds_end_value + irq_microseconds_end_value + soft_irq_microseconds_end_value + steal_microseconds_end_value + guest_microseconds_end_value - user_microseconds_start_value - nice_microseconds_start_value - system_microseconds_start_value - idle_microseconds_start_value - io_wait_microseconds_start_value - irq_microseconds_start_value - soft_irq_microseconds_start_value - steal_microseconds_start_value - guest_microseconds_start_value)//(1000*1000) AS total_cpu_time from v_internal.dc_cpu_aggregate_by_hour WHERE  start_time > (now() - cast('25 hours' AS interval)) ), B as ( select A.node_name, A.t_start_time, A.t_end_time, sum(A.nice_cpu_time) AS t_nice_cpu_time, sum(A.system_cpu_time) AS t_system_cpu_time, sum(A.io_wait_cpu_time) AS t_io_wait_cpu_time, sum(A.total_cpu_time) AS t_total_cpu_time from A group by A.t_start_time, A.t_end_time, A.node_name order by A.t_start_time, A.t_end_time, A.node_name asc ) SELECT /*+label(vBuddyLite)*/ b1.node_name, b1.t_start_time, b1.t_end_time, cast(b1.t_nice_cpu_time/(1000*1000*3600) as decimal(4,2)) as nice_cpu_percent, cast(b1.t_system_cpu_time/(1000*1000*3600) as decimal(4,2)) as system_cpu_percent, cast(b1.t_io_wait_cpu_time/(1000*1000*3600) as decimal(4,2)) as io_wait_cpu_percent FROM B b1 LEFT OUTER JOIN B b2     ON b1.node_name = b2.node_name and b1.t_system_cpu_time < b2.t_system_cpu_time WHERE b2.t_system_cpu_time IS NULL order by b1.node_name, b1.t_start_time; ###== Busiest in terms of IOWAIT% ==##  with A as ( select node_name, timestamp_trunc(start_time, 'HH') AS t_start_time, timestamp_trunc(end_time, 'HH') AS t_end_time, (nice_microseconds_end_value - nice_microseconds_start_value) AS nice_cpu_time, (system_microseconds_end_value - system_microseconds_start_value) AS system_cpu_time, (io_wait_microseconds_end_value - io_wait_microseconds_start_value) AS io_wait_cpu_time, (user_microseconds_end_value + nice_microseconds_end_value + system_microseconds_end_value + idle_microseconds_end_value + io_wait_microseconds_end_value + irq_microseconds_end_value + soft_irq_microseconds_end_value + steal_microseconds_end_value + guest_microseconds_end_value - user_microseconds_start_value - nice_microseconds_start_value - system_microseconds_start_value - idle_microseconds_start_value - io_wait_microseconds_start_value - irq_microseconds_start_value - soft_irq_microseconds_start_value - steal_microseconds_start_value - guest_microseconds_start_value)//(1000*1000) AS total_cpu_time from v_internal.dc_cpu_aggregate_by_hour WHERE  start_time > (now() - cast('25 hours' AS interval)) ), B as ( select A.node_name, A.t_start_time, A.t_end_time, sum(A.nice_cpu_time) AS t_nice_cpu_time, sum(A.system_cpu_time) AS t_system_cpu_time, sum(A.io_wait_cpu_time) AS t_io_wait_cpu_time, sum(A.total_cpu_time) AS t_total_cpu_time from A group by A.t_start_time, A.t_end_time, A.node_name order by A.t_start_time, A.t_end_time, A.node_name asc ) SELECT /*+label(vBuddyLite)*/ b1.node_name, b1.t_start_time, b1.t_end_time, cast(b1.t_nice_cpu_time/(1000*1000*3600) as decimal(4,2)) as nice_cpu_percent, cast(b1.t_system_cpu_time/(1000*1000*3600) as decimal(4,2)) as system_cpu_percent, cast(b1.t_io_wait_cpu_time/(1000*1000*3600) as decimal(4,2)) as io_wait_cpu_percent FROM B b1 LEFT OUTER JOIN B b2     ON b1.node_name = b2.node_name and b1.t_io_wait_cpu_time < b2.t_io_wait_cpu_time WHERE b2.t_io_wait_cpu_time IS NULL order by b1.node_name, b1.t_start_time ; ###== DETAILED CPU USAGE DURING BUSIEST HOURS (NICE% Only) ==##   with A as (  select  node_name,  timestamp_trunc(start_time, 'HH') AS t_start_time,  timestamp_trunc(end_time, 'HH') AS t_end_time,  (nice_microseconds_end_value - nice_microseconds_start_value) AS nice_cpu_time,  (system_microseconds_end_value - system_microseconds_start_value) AS system_cpu_time,  (io_wait_microseconds_end_value - io_wait_microseconds_start_value) AS io_wait_cpu_time,  (user_microseconds_end_value + nice_microseconds_end_value + system_microseconds_end_value + idle_microseconds_end_value + io_wait_microseconds_end_value + irq_microseconds_end_value + soft_irq_microseconds_end_value + steal_microseconds_end_value + guest_microseconds_end_value - user_microseconds_start_value - nice_microseconds_start_value - system_microseconds_start_value - idle_microseconds_start_value - io_wait_microseconds_start_value - irq_microseconds_start_value - soft_irq_microseconds_start_value - steal_microseconds_start_value - guest_microseconds_start_value)//(1000*1000)  AS total_cpu_time  from v_internal.dc_cpu_aggregate_by_hour  WHERE  start_time > (now() - cast('25 hours' AS interval))  ), B as (  select A.node_name, A.t_start_time, A.t_end_time, sum(A.nice_cpu_time) AS t_nice_cpu_time, sum(A.system_cpu_time) AS t_system_cpu_time, sum(A.io_wait_cpu_time) AS t_io_wait_cpu_time, sum(A.total_cpu_time) AS t_total_cpu_time  from A  group by A.t_start_time, A.t_end_time, A.node_name  order by A.t_start_time, A.t_end_time, A.node_name asc  ), C AS ( SELECT b1.node_name, b1.t_start_time FROM B b1 LEFT OUTER JOIN B b2     ON b1.t_nice_cpu_time < b2.t_nice_cpu_time WHERE b2.t_nice_cpu_time IS NULL Limit 1 ) SELECT /*+label(vBuddyLite)*/ Timestamp_trunc(start_time, 'MI') AS start_time,         dc_cpu_aggregate_by_minute.node_name,        CAST((nice_microseconds_end_value - nice_microseconds_start_value)/(60*1000*1000) as decimal(4,2)) AS nice_cpu_percent,        CAST((system_microseconds_end_value - system_microseconds_start_value)/(60*1000*1000) as decimal(4, 2)) AS system_cpu_percent,        CAST((io_wait_microseconds_end_value - io_wait_microseconds_start_value)/(60*1000*1000) as decimal(4,2)) AS io_wait_cpu_percent                      FROM   dc_cpu_aggregate_by_minute JOIN C        ON start_time > C.t_start_time and start_time <= (C.t_start_time + CAST('60 minutes' AS INTERVAL)) WHERE  dc_cpu_aggregate_by_minute.node_name = C.node_name order by start_time asc ;

[2.OS and System Information (Local Node Only)]
Linux Version Information|\! cat /proc/version###\!  cat /etc/{redhat,SuSE,debian,os,fedora,slackware,mandrake,yellowdog,sun,gentoo}{-,}release 2>/dev/null
//Block Device Readahead and Scheduler|NOTE: If this does not work, then run 'blockdev --report' as the super user to get the readahead \! lsblk --ascii --nodeps --output "NAME,FSTYPE,MOUNTPOINT,RA,SCHED"
Block Device Readahead|\! ( ls /sys/block/**/queue/read_ahead_kb | while read dev ; do DEVICE_NAME="${dev%/queue/read_ahead_kb}"; DEVICE_NAME="${DEVICE_NAME#/sys/block/}"; echo -n "${DEVICE_NAME}: " ; cat ${dev} ; done )
Block Device Scheduler|\! ( ls /sys/block/**/queue/scheduler | while read dev ; do DEVICE_NAME="${dev%/queue/scheduler}"; DEVICE_NAME="${DEVICE_NAME#/sys/block/}"; echo -n "${DEVICE_NAME}: " ; cat ${dev} ; done )
Transparent Hugepages Status|\! cat /sys/kernel/mm/{redhat_,}transparent_hugepage/enabled 2>/dev/null
Memory (in MB)|\!free -m
//Disk Usage (in bytes)|\!df
Disk Usage (human readable)|\!df -h

[3.Vertica Configuration]
Node Details|SELECT /*+label(vBuddyLite)*/ * FROM nodes ORDER BY 1;
Non Default Configuration Parameters|SELECT /*+label(vBuddyLite)*/ parameter_name, node_name, current_value, change_under_support_guidance, change_requires_restart, description, default_value FROM configuration_parameters WHERE current_value <> default_value GROUP BY parameter_name, node_name, current_value, change_under_support_guidance, change_requires_restart, description, default_value ORDER BY parameter_name, node_name ASC;
//Default Configuration|SELECT /*+label(vBuddyLite)*/ parameter_name, description, default_value FROM configuration_parameters ORDER BY parameter_name, node_name ASC;
Resource Pools|== CUSTOM RESOURCE POOLS ==##SELECT /*+label(vBuddyLite)*/ * FROM resource_pools WHERE NOT is_internal ORDER BY NAME;###== SYSTEM RESOURCE POOLS ==##SELECT /*+label(vBuddyLite)*/ * FROM resource_pools WHERE is_internal ORDER BY NAME;###== QUERY LATENCY BY RESOURCE POOL (LAST 24 HOURS) ==##SELECT /*+label(vBuddyLite)*/ pool_name 	,count(*) AS query_count 	,MIN(start_time) AS min_start_time 	,MAX(start_time) AS max_start_time 	,SUM(runtime_sec)::INT AS total_runtime_sec 	,SUM(Waiting_sec)::INT AS total_wait_time_sec 	,(((SUM(Waiting_sec) / NULLIFZERO(SUM(runtime_sec)))::DECIMAL(10, 2)) * 100)::INT AS wait_percent FROM ( 	SELECT DATE_TRUNC('SECOND', start_timestamp)::TIMESTAMP (0) AS start_time 		,B.pool_name 		,(A.request_duration_ms / 1000) AS runtime_sec 		,A.memory_acquired_MB 		,DATEDIFF('SECOND', B.resource_start_time, B.resource_grant_time) AS waiting_sec 	FROM query_requests AS A 	INNER JOIN ( 		SELECT transaction_id 			,statement_id 			,MIN(start_time) AS resource_start_time 			,MAX("time") AS resource_grant_time 			,(MAX(memory_kb) / 1024)::INT AS memory_MB 			,MAX(pool_name) AS pool_name 		FROM dc_resource_acquisitions 		WHERE result = 'Granted' 			AND request_type <> 'AcquireAdditional' 		GROUP BY 1 			,2 		) AS B ON ( 			A.transaction_id = B.transaction_id 			AND A.statement_id = B.statement_id 			) 	WHERE success = 't' 		AND A.request_type = 'QUERY' 		AND B.pool_name ILIKE '%' 		AND A.request_duration_ms > 0 		AND A.start_timestamp > NOW() - CAST('1 DAY' AS INTERVAL) 	) AS C GROUP BY 1 ORDER BY 1;
Is Local Segmentation On?|SELECT /*+label(vBuddyLite)*/ is_local_segment_enabled FROM elastic_cluster;
Total Storage Consumption per Node|SELECT /*+label(vBuddyLite)*/ node_name, storage_usage, disk_space_free_percent, storage_path FROM disk_storage WHERE storage_usage LIKE '%DATA%' ORDER BY 1;
Total Memory Usage Per Node in Last 24 Hours|SELECT /*+label(vBuddyLite)*/ node_name, MAX(average_memory_usage_percent) AS max_memory_usage_percent_last_day, MIN(average_memory_usage_percent) AS min_memory_usage_percent_last_day, TO_CHAR((SELECT MAX(total_memory) FROM dc_memory_info)/1024^3,'999,990.99') as total_memory_GB FROM memory_usage WHERE end_time > NOW() - CAST('1 day' AS INTERVAL) GROUP BY node_name, total_memory_GB ORDER BY 1;
Catalog Size|SELECT /*+label(vBuddyLite)*/ a.node_name, b.TIME, TO_CHAR(SUM(a.total_memory - a.free_memory)/1024^2,'999,999,990.99') AS catalog_memory_MB FROM dc_allocation_pool_statistics a INNER JOIN ( SELECT node_name, date_trunc('SECOND', max(TIME)) AS TIME FROM dc_allocation_pool_statistics GROUP BY 1 ) b ON a.node_name = b.node_name AND date_trunc('SECOND', a.TIME) = b.TIME GROUP BY 1, 2 ORDER BY node_name ;

[4.Schemas and Tables]
List all Schemas|SELECT /*+label(vBuddyLite)*/ schema_name AS schema FROM schemata WHERE NOT is_system_schema ORDER BY 1;
Number of Tables and Views per Schema|SELECT /*+label(vBuddyLite)*/ a.schema, ZEROIFNULL(b.table_count) AS table_count, ZEROIFNULL(c.view_count) AS view_count FROM (SELECT schema_name AS schema FROM schemata WHERE NOT is_system_schema) AS a LEFT JOIN (SELECT table_schema AS schema, COUNT(table_name) AS table_count FROM tables GROUP BY table_schema) AS b ON a.schema = b.schema LEFT JOIN (SELECT table_schema AS schema, COUNT(table_name) AS view_count FROM views GROUP BY table_schema) AS c ON a.schema = c.schema ORDER BY 1;
Number of Partitioned Tables|SELECT /*+label(vBuddyLite)*/ COUNT(DISTINCT table_id) FROM tables WHERE NOT is_system_table AND NOT partition_expression = '';
Top 10 Tables with Highest Storage Consumption|SELECT /*+label(vBuddyLite)*/ table_name, TO_CHAR(Total_Size/1024^3, '9,999,990.99') AS Total_Size_GB FROM (SELECT projection_schema || '.' || anchor_table_name AS table_name, SUM(used_bytes) AS Total_Size FROM projection_storage GROUP BY table_name ORDER BY 2 DESC,1 LIMIT 10) AS a;
Top 10 Tables with Highest Number of Rows|NOTE: All projection counts include buddies or replicas##SELECT /*+label(vBuddyLite)*/ a.table_name, TO_CHAR(row_count, '9,999,999,999,999,999') AS row_count, column_count, projection_count, projection_count_segmented, projection_count_replicated, num_of_super, num_of_non_super FROM (SELECT DISTINCT projection_schema || '.' || anchor_table_name AS table_name, row_count FROM projections INNER JOIN (SELECT DISTINCT projection_id, SUM(row_count) OVER (PARTITION BY projection_id) AS row_count FROM projection_storage) AS storage ON storage.projection_id = projections.projection_id ORDER BY row_count DESC, table_name LIMIT 10) AS a JOIN (SELECT table_schema || '.' || table_name AS table_name, COUNT(*) AS column_count FROM columns GROUP BY table_schema, table_name) AS b ON a.table_name = b.table_name JOIN (SELECT projection_schema ||'.'|| anchor_table_name AS table_name, COUNT(projection_id) AS projection_count, SUM(CASE WHEN is_segmented THEN 1 ELSE 0 END) AS projection_count_segmented, SUM(CASE WHEN is_segmented THEN 0 ELSE 1 END) AS projection_count_replicated FROM projections GROUP BY projection_schema, anchor_table_name) AS c ON a.table_name = c.table_name JOIN (SELECT projection_schema ||'.'|| anchor_table_name AS table_name, SUM(CASE WHEN is_super_projection THEN 1 ELSE 0 END) AS num_of_super, SUM(CASE WHEN is_super_projection THEN 0 ELSE 1 END) AS num_of_non_super FROM projections GROUP BY projection_schema,anchor_table_name) AS d ON a.table_name = d.table_name ORDER BY 2 DESC,1;
Top 10 Tables with Highest Number of Columns|SELECT /*+label(vBuddyLite)*/ a.table_name, column_count, TO_CHAR(ZEROIFNULL(row_count), '9,999,999,999,999,999') AS row_count FROM (SELECT table_schema || '.' || table_name AS table_name, COUNT(*) AS column_count FROM columns GROUP BY table_schema, table_name ORDER BY column_count DESC, table_name LIMIT 10) AS a LEFT JOIN (SELECT table_name, MAX(row_count) AS row_count FROM (SELECT DISTINCT projection_schema || '.' || anchor_table_name AS table_name, row_count FROM projections INNER JOIN (SELECT DISTINCT projection_id, SUM(row_count) OVER (PARTITION BY projection_id) AS row_count FROM projection_storage) AS storage ON storage.projection_id = projections.projection_id ORDER BY table_name) AS c GROUP BY table_name) AS b ON a.table_name = b.table_name ORDER BY 2 DESC, 1;
Tables with PK Restrictions|== NUMBER OF PK RESTRICTIONS ==##SELECT /*+label(vBuddyLite)*/ count(1) FROM constraint_columns WHERE constraint_type = 'p';###== FIRST 20 PK RESTRICTIONS ==##SELECT /*+label(vBuddyLite)*/ table_schema ||'.'|| table_name AS table_name, column_name FROM constraint_columns WHERE constraint_type = 'p' ORDER BY 1,2 LIMIT 20;
Tables with FK Restrictions|== NUMBER OF FK RESTRICTIONS ==##SELECT /*+label(vBuddyLite)*/ count(1) FROM constraint_columns WHERE constraint_type = 'f';###== FIRST 20 FK RESTRICTIONS ==##SELECT /*+label(vBuddyLite)*/ DISTINCT table_schema || '.' || table_name AS FK_table_name, column_name AS FK_column, reference_table_schema || '.' || reference_table_name as reference_table_name, reference_column_name as reference_column FROM constraint_columns WHERE constraint_type = 'f' ORDER BY 1,2 LIMIT 20;
//List of Tables with PK Restrictions|SELECT /*+label(vBuddyLite)*/ table_schema ||'.'|| table_name AS table_name, column_name FROM constraint_columns WHERE constraint_type = 'p' ORDER BY 1,2;
//List of Tables with FK Restrictions|SELECT /*+label(vBuddyLite)*/ DISTINCT table_schema || '.' || table_name AS FK_table_name, column_name AS FK_column, reference_table_schema || '.' || reference_table_name as reference_table_name, reference_column_name as reference_column FROM constraint_columns WHERE constraint_type = 'f' ORDER BY 1,2;
//Compression Ratio by Table|SELECT /*+label(vBuddyLite)*/ AUDIT('','TABLE'); SELECT /*+label(vBuddyLite)*/ ua.object_schema ||'.'|| ua.object_name AS table_name, size_bytes, compressed_size, CAST(size_bytes/compressed_size AS DECIMAL(14,2)) AS ratio FROM (SELECT projection_schema, anchor_table_name, SUM(ros_used_bytes) AS compressed_size FROM projection_storage GROUP BY projection_schema, anchor_table_name) AS ps INNER JOIN user_audits AS ua ON (ps.projection_schema = ua.object_schema AND ps.anchor_table_name = ua.object_name) WHERE audit_start_timestamp > '<<INTERNAL_stored_timestamp>>' AND ua.object_type = 'TABLE' ORDER BY table_name;
Top 20 Tables with Highest Query Utilization|== NON-SYSTEM TABLES ONLY ==##SELECT /*+label(vBuddyLite)*/ table_schema || '.' || table_name AS table_name, ROUND(tcount/total*100, 0.0) AS 'usage (%)' FROM (SELECT table_schema, table_name, count(*) AS tcount FROM v_internal.dc_projections_used WHERE table_schema not in ('v_internal','v_monitor', 'v_catalog') GROUP BY table_schema,table_name) AS t, ( SELECT count(*) AS total FROM v_internal.dc_projections_used WHERE table_name <> '' AND table_schema not in ('v_internal','v_monitor','v_catalog')) AS all_queries WHERE tcount > 0 ORDER BY 2 DESC, 1 LIMIT 20;###== INCLUDING SYSTEM TABLES ==##SELECT /*+label(vBuddyLite)*/ table_schema || '.' || table_name AS table_name, ROUND(tcount/total*100, 0.0) AS 'usage (%)' FROM (SELECT table_schema, table_name, count(*) AS tcount FROM v_internal.dc_projections_used GROUP BY table_schema,table_name) AS t, ( SELECT count(*) AS total FROM v_internal.dc_projections_used WHERE table_name <> '') AS all_queries WHERE tcount > 0 ORDER BY 2 DESC, 1 LIMIT 20;

[5.Projections]
Total Number of Projections|NOTE: All projection counts include buddies or replicas##SELECT /*+label(vBuddyLite)*/ COUNT(projection_id) AS total_proj_count, SUM(CASE WHEN create_type = 'DESIGNER' THEN 1 ELSE 0 END) AS DBD_created_proj_count FROM projections;
Top 10 Projections with Highest Storage Consumption|SELECT /*+label(vBuddyLite)*/ anchor_table_name, projection_basename, TO_CHAR(Total_Size_Incl_Buddies/1024^3, '9,999,999,990.99') AS Total_Size_Incl_Buddies_GB FROM (SELECT DISTINCT a.projection_schema || '.' || a.anchor_table_name AS anchor_table_name, a.projection_schema || '.' || b.projection_basename AS projection_basename, SUM(used_bytes) AS Total_Size_Incl_Buddies FROM projection_storage as a JOIN projections AS b ON a.projection_id = b.projection_id GROUP BY a.projection_schema, a.anchor_table_name, b.projection_basename ORDER BY Total_Size_Incl_Buddies DESC LIMIT 10) AS a;
Top 10 Projections with Most Bytes per Row|SELECT /*+label(vBuddyLite)*/ projection_basename, rows, bytes, (bytes/NULLIFZERO(rows))::INT AS bytes_per_row FROM (SELECT a.projection_schema || '.' || projection_basename AS projection_basename, SUM(row_count) AS rows, SUM(used_bytes) AS bytes FROM projection_storage AS a JOIN projections AS b ON a.projection_id = b.projection_id GROUP BY a.projection_schema, b.projection_basename) AS sq ORDER BY bytes_per_row DESC, projection_basename LIMIT 10;
Top 10 Tables with Highest Number of Projections|NOTE: All projection counts include buddies or replicas##== TOP 10 TABLES BY TOTAL PROJECTION COUNT ==##SELECT /*+label(vBuddyLite)*/ projection_schema || '.' || anchor_table_name AS table_name, COUNT(projection_id) AS projection_count, SUM(CASE WHEN is_segmented THEN 1 ELSE 0 END) AS projection_count_segmented, SUM(CASE WHEN is_segmented THEN 0 ELSE 1 END) AS projection_count_replicated FROM projections GROUP BY projection_schema, anchor_table_name ORDER BY 2 DESC, 1 LIMIT 10;###== TOP 10 TABLES BY SEGMENTED PROJECTION COUNT ==##SELECT /*+label(vBuddyLite)*/ projection_schema || '.' || anchor_table_name AS table_name, COUNT(projection_id) AS projection_count, SUM(CASE WHEN is_segmented THEN 1 ELSE 0 END) AS projection_count_segmented, SUM(CASE WHEN is_segmented THEN 0 ELSE 1 END) AS projection_count_replicated FROM projections GROUP BY projection_schema, anchor_table_name ORDER BY 3 DESC, 1 LIMIT 10;
Number of Tables with Segmented/Replicated Projections|SELECT /*+label(vBuddyLite)*/ SUM(CASE WHEN is_segmented THEN 0 ELSE 1 END) AS Replicated_Count, SUM(CASE WHEN is_segmented THEN 1 ELSE 0 END) AS Segmented_Count FROM (SELECT DISTINCT anchor_table_id, is_segmented FROM projections) AS a;
Maximum Number of Rows in a Replicated Projection|SELECT /*+label(vBuddyLite)*/ replicated_table_name, TO_CHAR(max_row_count,'999,999,999,999,999,999') AS max_row_count FROM (SELECT projection_schema || '.' || anchor_table_name AS replicated_table_name, row_count AS max_row_count FROM projections INNER JOIN (SELECT projection_id, SUM(row_count) OVER (PARTITION BY projection_id) AS row_count FROM projection_storage) AS storage ON storage.projection_id = projections.projection_id WHERE NOT is_segmented ORDER BY row_count DESC LIMIT 1) AS a;
Unused Projections in the Last Week|== NUMBER OF UNUSED PROJECTIONS ==##SELECT /*+label(vBuddyLite)*/ count(1) FROM (SELECT table_schema || '.' || projection_name AS projection_name, MAX(time) AS max_time FROM dc_projections_used WHERE table_schema NOT IN ('v_internal', 'v_monitor', 'v_catalog') GROUP BY table_schema, projection_name) AS a WHERE max_time < sysdate() - '1 week'::INTERVAL;###== OLDEST 20 UNUSED PROJECTIONS ==##SELECT /*+label(vBuddyLite)*/ projection_name, date_trunc('SECOND', max_time) AS last_time_used FROM (SELECT table_schema || '.' || projection_name AS projection_name, MAX(time) AS max_time, MIN(time) AS min_time FROM dc_projections_used WHERE table_schema NOT IN ('v_internal', 'v_monitor', 'v_catalog') GROUP BY table_schema, projection_name) AS a WHERE max_time < sysdate() - '1 week'::INTERVAL ORDER BY 2 LIMIT 20;
Non up-to-date Projections|== NUMBER OF NON UP-TO-DATE PROJECTIONS ==##SELECT /*+label(vBuddyLite)*/ count(DISTINCT projection_name) FROM projections WHERE NOT is_up_to_date;###== FIRST 20 NON UP-TO-DATE PROJECTIONS ALPHABETICALLY ==##SELECT /*+label(vBuddyLite)*/ DISTINCT projection_name FROM projections WHERE NOT is_up_to_date ORDER BY 1;
Tables with Projections with no Statistics|== NUMBER OF TABLES WITH PROJECTIONS WITH NO STATISTICS ==##SELECT /*+label(vBuddyLite)*/ COUNT(DISTINCT projection_schema || '.' || anchor_table_name) FROM projections WHERE NOT has_statistics ORDER BY 1;###== FIRST 20 TABLES WITH PROJECTIONS WITH NO STATISTICS BUT WITH OVER 1 MILLION RECORDS ==## WITH A AS ( SELECT /*+label(vBuddyLite)*/ DISTINCT projection_schema || '.' || anchor_table_name AS anchor_table_name, projection_id FROM projections WHERE NOT has_statistics ORDER BY 1 ), B AS ( SELECT DISTINCT projection_id, Sum(row_count) OVER (partition BY projection_id) AS row_count FROM            projection_storage ) select A.anchor_table_name, B.row_count from A        JOIN B ON A.projection_id = B.projection_id where B.row_count >= 1000000 order by B.row_count desc limit 20 ;
Number of Super/Non-Super Projections for Top 10 Tables by Row Count|SELECT /*+label(vBuddyLite)*/ DISTINCT a.anchor_table_name, num_of_super, num_of_non_super, TO_CHAR(row_count,'999,999,999,999,999') AS row_count FROM (SELECT projection_schema ||'.'|| anchor_table_name AS anchor_table_name, SUM(CASE WHEN is_super_projection THEN 1 ELSE 0 END) AS num_of_super, SUM(CASE WHEN is_super_projection THEN 0 ELSE 1 END) AS num_of_non_super FROM projections GROUP BY projection_schema, anchor_table_name) AS a JOIN (SELECT DISTINCT projection_schema || '.' || anchor_table_name AS table_name, row_count FROM projections INNER JOIN (SELECT DISTINCT projection_id, SUM(row_count) OVER (PARTITION BY projection_id) AS row_count FROM projection_storage) AS storage ON storage.projection_id = projections.projection_id ORDER BY row_count DESC, table_name LIMIT 10) AS b ON a.anchor_table_name = b.table_name ORDER BY row_count DESC, 1;
Data Skew In Top 10 Tables with Highest Row Count|== SKEW SUMMARY ==## SELECT /*+label(vBuddyLite)*/ anchor_table_name 	,MAX(skew_percent) AS max_skew_percent FROM ( 	SELECT anchor_table_schema || '.' || anchor_table_name AS anchor_table_name 		,projection_schema || '.' || projection_name AS projection_name 		,node_name 		,row_count 		,TO_CHAR(ABS( 		COALESCE((row_count - (SUM(row_count) OVER (PARTITION BY projection_id)) / (COUNT(row_count) OVER (PARTITION BY projection_id))) 		/ 		NULLIF((SUM(row_count) OVER (PARTITION BY projection_id)), 0), 0) 		* 100), '9,990.99') AS skew_percent 	FROM projection_storage 	WHERE anchor_table_schema || '.' || anchor_table_name IN ( 			SELECT table_name 			FROM ( 				SELECT DISTINCT projection_schema || '.' || anchor_table_name AS table_name 					,row_count 				FROM projections 				INNER JOIN ( 					SELECT DISTINCT projection_id 						,SUM(row_count) OVER (PARTITION BY projection_id) AS row_count 					FROM projection_storage 					) AS storage 					ON storage.projection_id = projections.projection_id 				ORDER BY row_count DESC LIMIT 10 				) AS aa 			) 	) AS bb GROUP BY anchor_table_name ORDER BY 2 DESC 	,1;   ###== SKEW PER PROJECTION PER NODE (TOP 20) ==## SELECT /*+label(vBuddyLite)*/ anchor_table_name 	,projection_name 	,node_name 	,MAX(skew_percent) AS skew_percent FROM ( 	SELECT anchor_table_schema || '.' || anchor_table_name AS anchor_table_name 		,projection_schema || '.' || projection_name AS projection_name 		,node_name 		,row_count 		,TO_CHAR(ABS( 		COALESCE((row_count - (SUM(row_count) OVER (PARTITION BY projection_id)) / (COUNT(row_count) OVER (PARTITION BY projection_id))) 		/ 		NULLIF((SUM(row_count) OVER (PARTITION BY projection_id)), 0), 0) 		* 100), '9,990.99') AS skew_percent 	FROM projection_storage 	WHERE anchor_table_schema || '.' || anchor_table_name IN ( 			SELECT table_name 			FROM ( 				SELECT DISTINCT projection_schema || '.' || anchor_table_name AS table_name 					,row_count 				FROM projections 				INNER JOIN ( 					SELECT DISTINCT projection_id 						,SUM(row_count) OVER (PARTITION BY projection_id) AS row_count 					FROM projection_storage 					) AS storage 					ON storage.projection_id = projections.projection_id 				ORDER BY row_count DESC LIMIT 10 				) AS aa 			) 	) AS bb WHERE skew_percent > 0 GROUP BY anchor_table_name 	,projection_name 	,node_name ORDER BY 4 DESC 	,1 	,2 	,3 LIMIT 20; 
Count of Tuple Mover Events in the Last Hour|select  /*+label(vBuddyLite)*/  count(*)  from  dc_tuple_mover_events where time > NOW() - CAST('1 hour' AS INTERVAL);

[6.Containers]
WOS and ROS Row Count and Used Bytes by Node|NOTE: WOS_used_GB is calculated separately because it is affected by subquery execution##SELECT /*+label(vBuddyLite)*/  node_name, SUM(region_in_use_size_kb)/1024^2 AS wos_used_GB FROM wos_container_storage GROUP BY node_name ORDER BY 1;###SELECT /*+label(vBuddyLite)*/ node_name, TO_CHAR(SUM(wos_row_count),'999,999,999,999,999') AS WOS_row_count, TO_CHAR(SUM(ros_row_count),'999,999,999,999,999') AS ROS_row_count, TO_CHAR(SUM(ros_used_bytes)/1024^3, '9,999,990.99') AS ROS_used_GB FROM column_storage GROUP BY node_name ORDER BY 1;
//WOS and ROS Row Count and Used Bytes by Node|SELECT /*+label(vBuddyLite)*/ a.node_name, TO_CHAR(wos_row_count,'999,999,999,999,999') AS WOS_row_count, TO_CHAR(wos_used_GB, '9,999,990.99') AS WOS_used_GB, TO_CHAR(ros_row_count,'999,999,999,999,999') AS ROS_row_count, TO_CHAR(ros_used_GB, '9,999,990.99') AS ROS_used_GB FROM (SELECT node_name, SUM(wos_row_count) AS wos_row_count, SUM(ros_row_count) AS ros_row_count, SUM(ros_used_bytes)/1024^3 AS ros_used_GB FROM column_storage GROUP BY node_name ORDER BY 1) AS a FULL OUTER JOIN (SELECT node_name, SUM(region_in_use_size_kb)/1024^2 AS wos_used_GB FROM wos_container_storage GROUP BY node_name ORDER BY 1) AS b ON a.node_name = b.node_name ORDER BY 1;
Top 10 Tables with Highest Number of ROS Containers|SELECT /*+label(vBuddyLite)*/ projection_schema ||'.'|| anchor_table_name as table_name, SUM(ros_count) AS ROS_count FROM projection_storage GROUP BY projection_schema, anchor_table_name ORDER BY ROS_count DESC LIMIT 10;
Top 10 Projections with Highest Number of ROS Containers|SELECT /*+label(vBuddyLite)*/ projection_schema ||'.'|| projection_name AS projection_name, SUM(ros_count) AS ROS_count FROM projection_storage GROUP BY projection_schema, projection_name ORDER BY ROS_count DESC LIMIT 10;
Top 10 Tables with Highest Number of Delete Vectors|SELECT /*+label(vBuddyLite)*/ projection_schema || '.' || anchor_table_name AS anchor_table_name, SUM(delete_vector_count) AS number_of_delete_vectors FROM projections JOIN (SELECT /*+label(vBuddyLite)*/ schema_name, projection_name, COUNT(*) AS delete_vector_count FROM delete_vectors GROUP BY schema_name, projection_name) AS a ON projections.projection_schema = a.schema_name AND projections.projection_name = a.projection_name GROUP BY projection_schema,anchor_table_name ORDER BY 2 DESC LIMIT 20;

[7.System Events]
Latest 20 Active Events|SELECT /*+label(vBuddyLite)*/ * FROM active_events ORDER BY event_posted_timestamp DESC LIMIT 20;
Errors in the Last 24 Hours|== NUMBER OF ERRORS BY ERROR CODE ==##SELECT /*+label(vBuddyLite)*/ error_code, COUNT(*) FROM error_messages WHERE event_timestamp > NOW() - CAST('1 week' AS INTERVAL) GROUP BY error_code ORDER BY error_code;###== LATEST ERROR FOR EACH ERROR CODE ==##SELECT /*+label(vBuddyLite)*/ error_code, event_timestamp, node_name, user_name, request_id, transaction_id, statement_id, error_level, message, detail, hint FROM (SELECT LAG(error_code,1,0) OVER(ORDER BY error_code, event_timestamp DESC) AS prev_error_code, * FROM error_messages WHERE event_timestamp > NOW() - CAST('1 week' AS INTERVAL) ORDER BY error_code, 2 DESC) AS a WHERE prev_error_code <> error_code;

//[G.DC Tables]

//[H.Profiles]

[8.Queries]
Top 10 Longest Running Queries in the Last Hour|SELECT /*+label(vBuddyLite)*/ b.start_timestamp,  b.transaction_id, b.statement_id, b.node_name, TO_CHAR(b.memory_acquired_mb,'999,999,990.99') AS memory_acquired_MB, TO_CHAR(b.request_duration_ms,'999,999,999,999') AS request_duration_MS, a.processed_row_count, b.request_label, b.request FROM query_profiles a, query_requests b WHERE a.transaction_id = b.transaction_id AND a.statement_id =b.statement_id AND b.is_executing='f' AND b.success AND b.start_timestamp > NOW() - '1 HOUR'::INTERVAL AND NOT b.request_label = 'vBuddyLite' AND b.request_type = 'QUERY' ORDER BY b.request_duration_ms DESC LIMIT 10;
Top 10 Most Frequent Queries in the last 24 Hours|SELECT /*+label(vBuddyLite)*/ COUNT(*) AS request_count, TO_CHAR(AVG(memory_acquired_mb),'999,990.99') AS avg_memory_acquired_MB, TO_CHAR(AVG(request_duration_ms),'999,990.99') AS avg_request_duration_MS, TO_CHAR(MAX(request_duration_ms),'999,990.99') AS max_request_duration_MS, request FROM query_requests WHERE start_timestamp > sysdate() - '1 DAY'::INTERVAL AND success AND NOT request_label = 'vBuddyLite' AND request_type = 'QUERY' GROUP BY request ORDER BY request_count DESC LIMIT 10;
Spill Events in the Last 24 Hours (limit 20)|SELECT /*+label(vBuddyLite)*/ qe.transaction_id, qe.statement_id, qe.event_type, qr.request_label, qr.request FROM (SELECT DISTINCT transaction_id, statement_id, event_type FROM query_events  WHERE event_type ILIKE '%SPILL%' AND event_timestamp > NOW() - CAST('1 day' AS INTERVAL) GROUP BY  transaction_id, statement_id, event_type) as qe JOIN query_requests AS qr ON (qe.transaction_id = qr.transaction_id AND qe.statement_id = qr.statement_id) WHERE NOT qr.request_label = 'vBuddyLite' ORDER BY 1 DESC, 2 LIMIT 20;
Query Events Summary for the Last 24 Hours|SELECT /*+label(vBuddyLite)*/ event_category, event_type, count(*) FROM query_events WHERE event_timestamp > NOW() - CAST('1 day' AS INTERVAL) GROUP by event_category, event_type;
//Spill Events in the Last 24 Hours|SELECT /*+label(vBuddyLite)*/ date_trunc('SECOND', qe.event_timestamp), qe.node_name, qe.event_type, qr.request_label, qr.request FROM (SELECT * FROM query_events WHERE event_type ILIKE '%SPILL%' AND event_timestamp > NOW() - CAST('1 day' AS INTERVAL)) AS qe JOIN query_requests AS qr ON (qe.transaction_id = qr.transaction_id AND qe.statement_id = qr.statement_id) WHERE NOT qr.request_label = 'vBuddyLite' ORDER BY 1 DESC, 2;
Query Max Resource Consumption|SELECT /*+label(vBuddyLite)*/ description, MAX(assigned_parallelism) AS max_threads, TO_CHAR(SUM(assigned_memory_bytes * assigned_parallelism)/(1024^2),'9,999,999,990.99') AS total_assigned_memory_MB FROM dc_plan_resources AS pr JOIN dc_plan_parallel_zones AS ppz  ON ( pr.plan_id = ppz.plan_id AND pr.parallel_zone_id = ppz.parallel_zone_id AND pr.transaction_id = ppz.transaction_id AND pr.statement_id = ppz.statement_id) GROUP BY description ORDER BY total_assigned_memory_MB DESC, description;
Query Runtime Stats in the Last 24 Hours|SELECT /*+label(vBuddyLite)*/ ( CASE WHEN memory_acquired_mb BETWEEN 0 AND 500 THEN 'SMALL (0-500MB RAM)' WHEN memory_acquired_mb BETWEEN 500.1 AND 2000 THEN 'MEDIUM (500MB-2GB RAM)' WHEN memory_acquired_mb > 2000 Then 'LARGE (>2GB RAM)' ELSE NULL END) AS Query_Type, COUNT(*), date_trunc('SECOND', MIN(start_timestamp)) AS earliest_timestamp, date_trunc('SECOND', MAX(start_timestamp)) AS latest_timestamp, MIN(request_duration_ms)//1000 AS MIN_Runtime_sec, AVG(request_duration_ms)//1000 As AVG_Runtime_sec, MAX(request_duration_ms)//1000 As MAX_Runtime_sec FROM query_requests WHERE success='t' AND start_timestamp BETWEEN  NOW() - CAST('1 day' AS INTERVAL) AND NOW() AND request_type='QUERY' AND memory_acquired_mb > 0 GROUP BY 1 ORDER BY 1;
Query Runtime Bucket Analysis in the Last 24 Hours|This query shows the distribution of queries in the last 24 hours, based on runtime. The buckets are defined as follows: Min value = 0, Max value = 1200 seconds, number of buckets = 10.##WITH rtime_detail AS (SELECT transaction_id, statement_id, MAX(request_duration_ms)/1000::INT AS query_runtime_sec FROM query_requests WHERE start_timestamp BETWEEN  NOW() - CAST('1 day' AS INTERVAL) AND NOW() AND SUCCESS='t' AND REQUEST_TYPE IN ('QUERY') AND request_duration_ms > 0 GROUP BY 1,2) SELECT /*+label(vBuddyLite)*/ COUNT(1) AS Query_Count, MAX(query_runtime_sec)::INT AS Max_query_runtime_sec, AVG(query_runtime_sec)::INT AS Avg_query_runtime_sec, MIN(query_runtime_sec)::INT AS Min_query_runtime_sec, SUM(query_runtime_sec)::INT AS Total_query_runtime_sec, WIDTH_BUCKET (query_runtime_sec, 0, 1200, 10) AS runtime_bucket FROM rtime_detail GROUP BY runtime_bucket ORDER BY runtime_bucket;
Runtime Stats per Job Category for the Last 24 Hours|SELECT /*+label(vBuddyLite)*/ request_type, COUNT(*) AS request_count, date_trunc('SECOND', MIN(start_timestamp)) AS earliest_request, date_trunc('SECOND', MAX(start_timestamp)) AS latest_request, SUM(request_duration_ms)//1000 As Total_Runtime_sec, (AVG(request_duration_ms))//1000 As AVG_Runtime_sec, MAX(request_duration_ms)//1000 As MAX_Runtime_sec FROM query_requests WHERE success='t' AND start_timestamp BETWEEN NOW() - CAST('1 day' AS INTERVAL) AND NOW() GROUP BY 1 ORDER BY 1;
Wait Time Breakdown for LOAD and QUERY Requests|SELECT /*+label(vBuddyLite)*/ request_type, date_trunc('SECOND', MIN(start_timestamp)) start_time, date_trunc('SECOND', MAX(start_timestamp)) end_time, ((SUM(execution_time_ms)/SUM(ZEROIFNULL(request_duration_ms)))*100)::INT AS Exec_Percent, (100 - ((SUM(execution_time_ms) / SUM(ZEROIFNULL(request_duration_ms))) * 100)::INT) AS Total_Wait_Percent, ((SUM(res_wait_time_ms) / SUM(ZEROIFNULL(request_duration_ms))) * 100)::INT AS Resource_Wait_Percent, ((SUM(queue_wait_time_ms) / SUM(ZEROIFNULL(request_duration_ms))) * 100)::INT AS Queue_Wait_Percent FROM (SELECT request_type, start_timestamp, transaction_id, statement_id, request_duration_ms, queue_wait_time_ms, res_wait_time_ms, execution_time_ms FROM (SELECT ri.node_name, ri.request_id, ri.transaction_id, ri.statement_id, ri.request_type, ROUND(ra.memory_mb, 2) AS memory_acquired_mb, rc.success, ri.time AS start_timestamp, ra.Resource_Start_Time AS Resource_Start_Time, ra.Resource_Grant_Time AS Resource_Grant_Time, rc.time AS end_timestamp, DATEDIFF('millisecond', ri.time, rc.time) AS request_duration_ms, DATEDIFF('millisecond', ri.time, ra.Resource_Start_Time) AS queue_wait_time_ms, DATEDIFF('millisecond', ra.Resource_Start_Time, ra.Resource_Grant_Time) AS res_wait_time_ms, DATEDIFF('millisecond', ra.Resource_Grant_Time, rc.time) AS execution_time_ms, rc.time IS NULL AS is_executing FROM v_internal.dc_requests_issued ri LEFT OUTER JOIN v_internal.dc_requests_completed rc USING (node_name, session_id, request_id) LEFT OUTER JOIN (SELECT node_name, session_id, request_id, COUNT(*) AS error_count FROM v_internal.dc_errors WHERE error_level >= 20 GROUP BY 1,2,3) de USING (node_name, session_id, request_id) LEFT OUTER JOIN (select node_name, transaction_id, statement_id, MIN(start_time) AS Resource_Start_Time, MAX ("time") AS Resource_Grant_Time, MAX(memory_kb)/1024::float AS memory_mb FROM v_internal.dc_resource_acquisitions WHERE result = 'Granted' GROUP BY 1,2,3) ra USING (node_name, transaction_id, statement_id) ) A WHERE request_type IN ('QUERY', 'LOAD') AND execution_time_ms > 0 ) B GROUP BY request_type ORDER BY 1;
Number of Queries by Type by Day in Last Week|SELECT /*+label(vBuddyLite)*/ start_timestamp::date AS date, CASE WHEN request_type = 'QUERY' THEN CASE WHEN regexp_like(request, '^\s*(profile\s+)*select\s', 'i') THEN 'SELECT' WHEN regexp_like(request, '^\s*(profile\s+)*insert\s', 'i') THEN 'INSERT' WHEN regexp_like(request, '^\s*(profile\s+)*update\s', 'i') then 'UPDATE' WHEN regexp_like(request, '^\s*(profile\s+)*delete\s', 'i') then 'DELETE' WHEN regexp_like(request, '^\s*(profile\s+)*merge\s', 'i') then 'MERGE' ELSE 'OTHER' END ELSE request_type END AS qtype, count(*) AS count FROM query_requests WHERE start_timestamp > NOW() - 7 GROUP BY 1, 2 ORDER BY 1 DESC, 2;
Export Last Week's Queries for Database Designer|SELECT /*+label(vBuddyLite)*/ 1 AS num, a.request FROM dc_requests_issued AS a JOIN (SELECT request_id, transaction_id,statement_id FROM query_requests WHERE request_type = 'QUERY' AND success AND start_timestamp > sysdate() - '7 days'::INTERVAL AND NOT request_label = 'vBuddyLite') AS b ON a.request_id = b.request_id AND a.transaction_id = b.transaction_id AND a.statement_id = b.statement_id ORDER BY 1;
Query Elapsed distribution (Last Week Only)|select  /*+label(vBuddyLite)*/  request_type,     sum(case when request_duration_ms < 1000 then 1 else 0 end) as 'less1s',     sum(case when request_duration_ms between 1000 and 2000 then 1 else 0 end) as '1to2s',     sum(case when request_duration_ms between 2000 and 5000 then 1 else 0 end) as '2to5s',     sum(case when request_duration_ms between 5000 and 10000 then 1 else 0 end) as '5to10s',     sum(case when request_duration_ms between 10000 and 20000 then 1 else 0 end) as '10to20s',     sum(case when request_duration_ms between 20000 and 60000 then 1 else 0 end) as '20to60s',     sum(case when request_duration_ms between 60000 and 120000 then 1 else 0 end) as '1to2m',     sum(case when request_duration_ms between 120000 and 300000 then 1 else 0 end) as '2to5m',     sum(case when request_duration_ms between 300000 and 600000 then 1 else 0 end) as '5to10m',     sum(case when request_duration_ms between 600000 and 6000000 then 1 else 0 end) as '10to60m',     sum(case when request_duration_ms >= 6000000 then 1 else 0 end) as 'more1h' from     v_monitor.query_requests  where     is_executing is false and start_timestamp > (now() - cast('1 WEEK' AS interval)) group by 1 ; 
Number of Active Querie Requests by the Minutes (During the last Hour, including those started and stopped in the same minute)| WITH ts_intervals AS (  SELECT ts::TIMESTAMP    FROM (          SELECT CAST(NOW() - CAST('1 hours' AS INTERVAL) AS TIMESTAMP) AS tm                             UNION            SELECT CAST(NOW() AS TIMESTAMP) AS tm           ) AS t TIMESERIES ts AS '1 MINUTE' OVER (                       ORDER BY tm                     )       ) SELECT /*+label(vBuddyLite)*/ ts_intervals.ts AS ts   ,count(*) FROM ts_intervals     ,v_monitor.query_requests WHERE (               start_timestamp <= ts_intervals.ts              AND end_timestamp >= ts_intervals.ts    )        OR (            start_timestamp > ts_intervals.ts               AND start_timestamp < TIMESTAMPADD(SQL_TSI_HOUR, 1, ts_intervals.ts)            ) GROUP BY ts_intervals.ts ORDER BY ts_intervals.ts; 
Epoch Status and Delete Vector Status|select /*+label(vBuddyLite)*/ 'Last Good Epoch'as epoch, epoch_number, epoch_close_time from v_monitor.system  inner join v_catalog.epochs on epoch_number = last_good_epoch  union all select 'Ancient History Mark' as epoch, epoch_number, epoch_close_time from v_monitor.system  inner join v_catalog.epochs on epoch_number = ahm_epoch  union all select 'Current Epoch' as epoch, current_epoch as epoch_number, epoch_close_time from v_monitor.system left outer join v_catalog.epochs on epoch_number = current_epoch  ;  ###== DELETE VECTOR STATUS ==##  select  /*+label(vBuddyLite)*/   start_epoch,     end_epoch,     storage_type,     sum(deleted_row_count) as sum_deleted_rows from      delete_vectors  group by 1, 2, 3; 

[9.DDLs]
Show Entire Catalog|SELECT /*+label(vBuddyLite)*/ EXPORT_CATALOG();
Show Specific Objects|SELECT /*+label(vBuddyLite)*/ EXPORT_OBJECTS( '' , '<<Scope>>');
Show Specific Tables|SELECT /*+label(vBuddyLite)*/ EXPORT_TABLES('', '<<Scope>>');
EXPORT ALL database DDLs (ENTIRE CATALOG) to a file|SELECT /*+label(vBuddyLite)*/ EXPORT_CATALOG();

[10.Hardware Tests]
CPU Performance Test (~ 1min)|\? /opt/vertica/bin/vcpuperf
I/O Performance Test - Quick (~ 1 min)|\? /opt/vertica/bin/vioperf --duration=60s <data_directory>
I/O Performance Test - Extensive (~ 10 min)|\? /opt/vertica/bin/vioperf --duration=10min <data_directory>
Network Performance Test|\? /opt/vertica/bin/vnetperf --output-file /tmp/temp.json

[11.Graphs]
CPU usage (1st node only)|select /*+label(vBuddyLite)*/ start_time, average_cpu_usage_percent from cpu_usage where node_name=(select node_name from nodes order by 1 limit 1) and start_time BETWEEN NOW() - CAST('1 day' AS INTERVAL) AND NOW() order by 1;
Memory usage (1st node only)|select /*+label(vBuddyLite)*/ start_time, average_memory_usage_percent from memory_usage where node_name=(select node_name from nodes order by 1 limit 1) and start_time BETWEEN NOW() - CAST('1 day' AS INTERVAL) AND NOW() order by 1;
Network Usage (1st node only)|select /*+label(vBuddyLite)*/ start_time, tx_kbytes_per_sec, rx_kbytes_per_sec from network_usage where node_name=(select node_name from nodes order by 1 limit 1) and start_time BETWEEN NOW() - CAST('1 day' AS INTERVAL) AND NOW() order by 1;
IO Usage (1st node only)|select /*+label(vBuddyLite)*/ start_time, read_kbytes_per_sec, written_kbytes_per_sec from io_usage where node_name=(select node_name from nodes order by 1 limit 1) and start_time BETWEEN NOW() - CAST('1 day' AS INTERVAL) AND NOW() order by 1;
"""

# Help Text for the HELP menu
vBuddyLiteMainHelp = """
Help on Using vBuddy Lite
*************************

vBuddy Lite, like Vertica adminTools, is implemented using Dialog, a graphical
user interface that works in terminal (character-cell) windows. If you are
not familiar with this interface, read the section 'Using Dialog' at the 
bottom of this help menu.

vBuddy Lite Menus
-----------------
vBuddy Lite contains a number of useful queries you can run against your 
cluster. They are grouped in a number of different menus. 

** Running a Single Query

In each menu selecting an item will run the query and display the output on 
the screen.

** Running Multiple Queries

In each menu the last option is to "CREATE REPORT from All Tests In This Menu"
Selecting this option will run ALL queries in the menu and save the results
to a file. 

** Running ALL Queries

If you select "CREATE FULL REPORT" from the main menu, vBuddy Lite will execute
all queries and save the results in a file. By default, DDLs and Last Week's 
queries are not included in the FULL REPORT. You will be asked if you also 
want those exported.

** Interactive Queries

Some queries (like the object DDL) will ask for some input before executing.
These queries are NOT included in the FULL REPORT. 

** vBuddy Lite Result Files

vBuddy Lite creates 4 files when it executes:
   ~/vBuddyLite-<username>-<nodename>.errors   //error log (should be empty!)
   ~/vBuddyLite-<username>-<nodename>.log      //normal log 
   ~/vBuddyLite-<username>-<nodename>.output   //Query Results
   ~/vBuddyLite-<username>-<nodename>.sql      //Last week's queries in correct
                                               //sql so it can be fed into DBD

** Copying Query or Results From The Screen

If you want to copy text from the screen hold down SHIFT and use your MOUSE 
to select the text you want to copy. 


Using Dialog
------------

The user interface responds to mouse clicks in some terminal windows, 
particularly local Linux windows, but you may find that it only responds to 
keystrokes.    
     
** Keystroke Quick Reference  

This quick reference does not describe every possible combination of         
keystrokes that can be used to accomplish a particular task. Feel free to    
experiment and to use whatever keystrokes you prefer.    
       
Return        Execute selected command.        
       
Tab Move cursor from OK to Cancel to Help to menu or     
    form to OK...
       
Up/Down Arrow Move cursor up and down in menu, form, or help       
    file.        
       
Space         Select item in list.   
       
character     Select corresponding command from menu.    
       
** Command Menu Dialogs       

Some dialogs require you to choose one command from a menu. Type the         
alphanumeric character shown or use the up and down arrow keys to select a   
command. When you have made your selection, press Return.
       
** Choose From List Dialogs   
        
In a list dialog, use the up and down arrow keys to highlight items, then    
use the space bar to select the items, marking them with an X. Some list     
dialogs allow you to select multiple items. when you have finished selecting 
items, press Return.       
       
** Form Dialogs    
 
In a form dialog, use the tab key to cycle between OK, Cancel, and the form  
field area. Once the cursor is in the form field area, use the up and down   
arrow keys to select an individual field (highlighted) and enter   
information. When you have finished entering information in all fields,      
press Return.    
     
** Password Authentication       

vBuddy Lite maintains a session context with regard to password authentication.
If you enter it correctly, you will not be asked to enter it again until you
exit and restart vBuddy Lite.   
       
** Between Dialogs  
 
While vBuddy Lite is working, you will see the command line processing. Do 
not interrupt the processing.   
       
** Notes  
         
The appearance of the graphical interface depends on the color and font      
settings used by your terminal window.  

Questions / Feedback
-------------------- 
Email or IM: ifigeneia.derekli@hp.com
"""

# HTML prefixes for the HTML output
html_pre="""
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta charset="UTF-8">
<link rel="Shortcut Icon" type="image/x-icon" href="https://my.vertica.com/wp-content/themes/vertica-2013/images/favicon.ico">
<title>vBuddyLiteReport</title>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script src="http://code.jquery.com/jquery-1.9.1.min.js"></script>
<script type="text/javascript">
google.load('visualization', '1', {packages: ['corechart', 'line']});
</script>
"""

# HTML style (CSS) for the HTML output
html_style="""
<style>
.container {
   border:1px solid #01b187;
   display: inline-block;
}

.container .header {
    background-color:#01b187;
    padding: 2px;
    cursor: pointer;
    font-weight: bold;
}
.container .content {
    display: none;
    padding : 5px;
}
ul {
    list-style-type: none;
}

.li2 {
    margin-left:30px;
}

li a{
    margin-left: 10px;
}

h1, h2, h3, h4, h5, p, td {
    color: #333c42;
    font-family: HP Simplified;
}
div{
font-family: HP Simplified Light;
}

td,th {
font-family: HP Simplified Light;
font-size:16px;
}
h2,h3{
color: #01b187;
}
h4{
 color: #5E5B54;
 }

.time {
    font-size:10px;
    font-color:grey;
}

.query {
    font-style: italic;
    font-family: courier new;
    margin-bottom:10px;
}

th{
    padding:5px;
    background-color:#EFEFEF;
}

td{
    padding:5px;
    background-color:#EFEFEF;
}
.contents{
    background-color:#EFEFEF;
    margin: 10px;
    padding: 10px;
    font-family: HP Simplified Light;
    border-radius: 10px;
    min-width:350px;
}

ul {
    padding-left:0px;

}
</style>
"""

# extra javascript t do the collapse/expand of results in HTML output
expandDataJS = """
<script type="text/javascript">
$(".header").click(function () {

    $header = $(this);
    //getting the next element
    $content = $header.next();
    //open up the content needed - toggle the slide- if visible, slide up, if not slidedown.
    $content.slideToggle(500, function () {
        //execute this after slideToggle is done
        //change text of header based on visibility of content div
        $header.text(function () {
            //change text based on condition
            return $content.is(":visible") ? "CLICK to collapse data" : "CLICK to expand data";
        });
    });

});
</script>
"""

VBUDDYLITE_TAG = ""     # global variable of menu # selected in GUI
VBUDDYLITE_MENU = ""    # global variable of current menu in GUI
EXPANDED = False        # whether to enable expanded view for SQL results in vsql

# creating Logs and Output file
vBuddyLiteHostName = socket.gethostname()
vBuddyLiteLogUser = pwd.getpwuid(os.getuid())[0]
# home = os.path.expanduser("~")
home = os.getcwd()
javascript = ''

vBuddyLiteLog = os.path.join(home , "vBuddyLite-%s-%s.log" % (vBuddyLiteLogUser, vBuddyLiteHostName))
vBuddyLiteOutputLog = os.path.join(home , "vBuddyLite-%s-%s.output" % (vBuddyLiteLogUser, vBuddyLiteHostName))
# vBuddyLiteOutputHtml = os.path.join(home , "vBuddyLite-%s-%s.html" % (vBuddyLiteLogUser, vBuddyLiteHostName))
vBuddyLiteOutputHtml = os.path.join(home , "vBuddyLite-%s-%s.html" % (vBuddyLiteLogUser, vBuddyLiteHostName))
vBuddyLiteErrorLog = os.path.join(home , "vBuddyLite-%s-%s.errors" % (vBuddyLiteLogUser, vBuddyLiteHostName))
vBuddyLiteQueriesOutputLog = os.path.join(home , "vBuddyLite-%s-%s.sql" % (vBuddyLiteLogUser, vBuddyLiteHostName))


# helper function to calculate the evaluate the hardware numbers  
def calcPerf(samples, evalArray, orderedPossibleResults, thres=2):
    results = list()
    for i in range(0,len(samples)):
        value = samples[i]
        evalDict = evalArray[i]
        result = ""
        for j in range(0,len(orderedPossibleResults)-1):
            if value < evalDict[orderedPossibleResults[j]]:
                result = orderedPossibleResults[j]
                if j < thres:
                    result = "**"+result+"**"
                break
        if (result == ""):
            result = orderedPossibleResults[len(orderedPossibleResults)-1]
        results.append(result)
        
    return results

# helper function to construct the javascript for the GRAPHS    
def getJavascript(str,q_id):
    q_id = q_id.replace('.','_')
    data_html = 'var data'+q_id +' = new google.visualization.DataTable();\n\n'
    data = ''
    columns = []
    for line in re.findall(r'<tr>(.*?)</tr>',str):
        if line.strip().startswith('<th>'):
            for c in re.findall(r'<th>(.*?)</th>',line):
                columns.append(c.strip())
            continue;
        line = re.sub(r'</td><td>',',',line)
        line = re.sub(r'<td>(.*?),','[new Date(\'\\1\'),',line)
        line = re.sub(r'</td>','],\n',line)
        data += line

    hAxis_title = ''
    if len(columns) > 1:
        hAxis_title = columns[0]
        
    first = True
    for col in columns:
        col = col.replace('_',' ')
        if first:
            data_html += "data"+q_id +".addColumn('datetime','"+col+"');\n"
            first = False
        else:
            data_html += "data"+q_id +".addColumn('number','"+col+"');\n"

    data_html += '\ndata'+q_id +'.addRows(['+data[:-2]+']);\n\n'
    data_html += "var options"+q_id +" = {hAxis: {title: '"+hAxis_title+"'},vAxis: {title: ''},series: {1: {curveType: 'function'}}};"
    data_html += "var chart"+q_id +" = new google.visualization.LineChart(document.getElementById('chart_div_"+q_id +"')); chart"+q_id +".draw(data"+q_id +", options"+q_id +");"

    return data_html

# helper function to convert the output to HTML formatting    
def htmlify(str,collapsed):

    collapse_html = '<div class="container"><div class="header">CLICK to collapse data</div><div class="content" style="display: block">'
    if collapsed:
            collapse_html = '<div class="container"><div class="header">CLICK to expand data</div><div class="content" style="display: none">'

    str=re.sub(r'(==.*?==)\n',r'<h4>\1</h4>',str)
    str=re.sub(r'\n(QUERY:.*?)\n',r'\n<div class="query">\1</div><table>',str)
    str=re.sub(r'(QUERY:.*?EXPORT_CATALOG\(\);</div>)<table>\n',r'\1',str)
    str=re.sub(r'\n(COMMAND:.*?)\n',r'\n<div class="query">\1</div>',str)
    str=re.sub(r'\n(\(\d+ row.?\))',r'</td></tr></table></div></div><br/>\1<br/>\n',str)
    str=re.sub(r'(Completed in.*?)\n',r'<br/>\1<br/>',str)
    str=re.sub(r'(Completed in.*?s)',r'\1<br/>',str)
    str=re.sub(r'<table>\n(.+?)\n',r'<table><tr><th>\1</th></tr><tr><td>\n',str)
    str=re.sub(r'<table>',collapse_html+'<table>',str)
    
    while re.search(r'.*?<th>.*?\|',str):
        str=re.sub(r'(<th>.*?)\|\s*',r'\1</th><th>',str)
    str=re.sub(r'\n[-+]+\n','',str)

    a = re.compile(r'<td>.*?\|[^<]*?\n')
    b = re.compile(r'(<td>.*?\|[^<]*?)\n')
    while re.search(a,str):
        str=re.sub(b,r'\1</td></tr><tr><td>',str)
    
    while re.search(r'<td>.*?\|',str):
        str=re.sub(r'(<td>.*?)\|\s*',r'\1</td><td>',str)
    
    while re.search(r'\n\n',str):
        str=re.sub(r'\n\n',r'\n',str)
    str=re.sub(r'\n','<br/>',str)
    str=re.sub(r'<tr><td><br/>[-+]+</td></tr>','',str)  
    
    return str

# helper function to calculate min/max/mean for VIOPERF results    
def calcMinMaxMeanIo(samples):
    min = 0
    max = 0
    mean = 0

    newSamples = list()
    for k,v in samples.items():
        #disregard top 2 and bottom 2 results
        v.sort()
        v.pop()
        v.pop()
        v.reverse()
        v.pop()
        v.pop()
        for i in v:
            newSamples.append(i)
        
    newSamples.sort()
        
    min = newSamples[0]
    max = newSamples[-1]
    mean = sum(newSamples) / len(newSamples)

    return (min, max, mean)

# helper function to calculate min/max/mean from an array    
def calcMinMaxMean(samples):
    min = 0
    max = 0
    mean = 0

    samples.sort()
    min = samples[0]
    max = samples[-1]
    mean = sum(samples) / len(samples)

    return (min, max, mean)

# helper function to format output table cells    
def formatNumAppendSpaces(number, spaces, decimals=1):
    a = str(Decimal(str(number)).quantize(Decimal(10) ** -decimals))
    
    for i in range (0,spaces-len(a)):
        a = " "+a
    
    return a

# helper function to format output table cells    
def appendSpaces(a, spaces):
    for i in range (0,spaces-len(a)):
        a = a + " "
    
    return a

# function used to parse and evaluate VNETPERF results
def parseVnetperfResult(output):
    latencyEvalMatrix = {'POOR': 400, 'MARGINAL': 200, 'GOOD': 100}
    clockSkewEvalMatrix = {'POOR': 2000000, 'MARGINAL': 1000000, 'GOOD': 500000}
    
    tcpEvalMatrix = {'POOR': 100, 'MARGINAL': 400, 'GOOD': 800}
    udpEvalMatrix = {'POOR': 100, 'MARGINAL': 200, 'GOOD': 400}
    
    percentageAcceptance = 20 #if a value deviates from the average or the rate more than this percentage, it is considered an error. Default is 10%
    
    reLatency = re.compile('latency\s+\|.+?\|\s+(.+?)\s+\|\s+(\d+?)\s+\|\s+(\d+?)\s+\|\s+(\-?\d+)')
    reUdp = re.compile('.+?udp-throughput.+?\|\s+(\d+?)\s+\|\s+(.+?)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+?)\s+\|\s+(\d+?)\s+\|\s+(\d+\.?\d*)')
    reUdpAverage = re.compile('.+?udp-throughput.+?\|\s+(\d+?)\s+\|\s+average\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+?)\s+\|\s+(\d+?)\s+\|\s+(\d+\.?\d*)')
    reTcp = re.compile('.+?tcp-throughput.+?\|\s+(\d+?)\s+\|\s+(.+?)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+?)\s+\|\s+(\d+?)\s+\|\s+(\d+\.?\d*)')
    reTcpAverage = re.compile('.+?tcp-throughput.+?\|\s+(\d+?)\s+\|\s+average\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+?)\s+\|\s+(\d+?)\s+\|\s+(\d+\.?\d*)')
   
   
    latencyPerf = "HIGH"
    clockSkewPerf = "HIGH"
    latencyValue = 0
    clockSkewValue = 0
    
    minDict = {}
    valuesUdpDict = {}
    valuesTcpDict = {}
    outliersDict = {}
    nodeIPs = list()
    for line in output.split('\n'):
        reMatch = reLatency.match(line)
        if (reMatch):
            nodeIP = reMatch.group(1)
            nodeIPs.append(nodeIP)
            latency = int(reMatch.group(3))
            skew = math.fabs(int(reMatch.group(4)))
            nodeLatencyPerf = "HIGH"
            nodeSkewPerf = "HIGH"
            if latency > latencyEvalMatrix['POOR']:
                nodeLatencyPerf = latencyPerf = "**POOR**"
            elif latency > latencyEvalMatrix['MARGINAL']:
                nodeLatencyPerf = "**MARGINAL**"
                if latencyPerf != "**POOR**":
                    latencyPerf = "**MARGINAL**"
            elif latency > latencyEvalMatrix['GOOD']:
                if latencyPerf == "HIGH":
                    latencyPerf = "GOOD"
            if latency > latencyValue:
                latencyValue = latency
                  
            if skew > clockSkewEvalMatrix['POOR']:
                nodeSkewPerf = clockSkewPerf = "**POOR**"
            elif skew > clockSkewEvalMatrix['MARGINAL']:
                nodeSkewPerf = "**MARGINAL**"
                if clockSkewPerf != "**POOR**":
                    clockSkewPerf = "**MARGINAL**"
            elif skew > clockSkewEvalMatrix['GOOD']:
                if clockSkewPerf == "HIGH":
                    clockSkewPerf = "GOOD"
            if skew > clockSkewValue:
                clockSkewValue = skew
            
            if latency > latencyEvalMatrix['MARGINAL'] or skew > clockSkewEvalMatrix['MARGINAL']:
                #add to "outliers" 
                if outliersDict.has_key(nodeIP):
                    outliersDict[nodeIP]['skew'] = nodeSkewPerf + appendSpaces(" ("+str(skew)+" us)",13)
                    outliersDict[nodeIP]['latency'] = nodeLatencyPerf + " (" + str(latency) + " us)"
                else:
                    outliersDict[nodeIP] = {'skew':nodeSkewPerf + appendSpaces(" ("+str(skew)+" us)",13), 'latency':nodeLatencyPerf + " (" + str(latency) + " us)"}
            continue
                
        reMatch = reUdp.match(line)
        if (reMatch):
            rateLimit = int(reMatch.group(1))
            nodeIP = reMatch.group(2)
            avgSentRec = (float(reMatch.group(3)) + float(reMatch.group(4))) / 2
            if valuesUdpDict.has_key(nodeIP):
                valuesUdpDict[nodeIP][rateLimit] = avgSentRec
            else:
                valuesUdpDict[nodeIP] = {rateLimit:avgSentRec}
            continue
        
        reMatch = reUdpAverage.match(line)
        if (reMatch):
            rateLimit = int(reMatch.group(1))
            avgSentRec = (float(reMatch.group(3)) + float(reMatch.group(2))) / 2
            if valuesUdpDict.has_key('average'):
                valuesUdpDict['average'][rateLimit] = avgSentRec
            else:
                valuesUdpDict['average'] = {rateLimit:avgSentRec}
            continue
     
        reMatch = reTcp.match(line)
        if (reMatch):
            rateLimit = int(reMatch.group(1))
            nodeIP = reMatch.group(2)
            avgSentRec = (float(reMatch.group(3)) + float(reMatch.group(4))) / 2
            if valuesTcpDict.has_key(nodeIP):
                valuesTcpDict[nodeIP][rateLimit] = avgSentRec
            else:
                valuesTcpDict[nodeIP] = {rateLimit:avgSentRec}
            continue
        
        reMatch = reTcpAverage.match(line)
        if (reMatch):
            rateLimit = int(reMatch.group(1))
            avgSentRec = (float(reMatch.group(3)) + float(reMatch.group(2))) / 2
            if valuesTcpDict.has_key('average'):
                valuesTcpDict['average'][rateLimit] = avgSentRec
            else:
                valuesTcpDict['average'] = {rateLimit:avgSentRec}
            continue
            
    
    #calc TCP and UDP perf
    tcpOverallPerf = "HIGH"
    udpOverallPerf = "HIGH"
    tcpMaxRate = 0
    udpMaxRate = 0
    
    cleanOutput = "The vnetperf tool was run in all "+str(len(nodeIPs))+" nodes simultaneously. Your network performance results are summarized below \n(Detailed test results available in the report):\n\n"
    cleanOutput_html = "The vnetperf tool was run in all "+str(len(nodeIPs))+" nodes simultaneously. Your network performance results are summarized below <br/>(Detailed test results available in the report):<br/><br/>"
    
    for k1 in sorted(valuesUdpDict['average']):
        percentToRate = math.fabs(valuesUdpDict['average'][k1]-k1)*100/k1
        if percentToRate > percentageAcceptance:
            udpMaxRate = valuesUdpDict['average'][k1]
            if udpMaxRate < udpEvalMatrix['POOR']:
                udpOverallPerf = "**POOR**"
            elif udpMaxRate < udpEvalMatrix['MARGINAL']:
                udpOverallPerf = "**MARGINAL**"
            elif udpMaxRate < udpEvalMatrix['GOOD']:
                udpOverallPerf = "GOOD"
            break

    for k1 in sorted(valuesTcpDict['average']):
        percentToRate = math.fabs(valuesTcpDict['average'][k1]-k1)*100/k1
        if percentToRate > percentageAcceptance:
            tcpMaxRate = valuesTcpDict['average'][k1]
            if tcpMaxRate < tcpEvalMatrix['POOR']:
                tcpOverallPerf = "**POOR**"
            elif tcpMaxRate < tcpEvalMatrix['MARGINAL']:
                tcpOverallPerf = "**MARGINAL**"
            elif tcpMaxRate < tcpEvalMatrix['GOOD']:
                tcpOverallPerf = "GOOD"
            break
    

    tempOut2 = ' Type |        IP        || Rate Limit (MB/s) | Cluster Sent/Rec Avg (MB/s) || Node Sent/Rec Avg (MB/s) | % Diff to Avg | % Diff to Rate\n'
    tempOut2 += '----------------------------------------------------------------------------------------------------------------------------------------\n' 
    tempOut2_html = '<table><tr><th>Type</th><th>IP</th><th>Rate Limit (MB/s)</th><th>Cluster Sent/Rec Avg (MB/s)</th><th>Node Sent/Rec Avg (MB/s)</th><th>% Diff to Avg</th><th>% Diff to Rate</th></tr>'
    
    tempOut1 = ""    
    tempOut1_html = ""
    
    for k in sorted(valuesUdpDict):
        for k1 in sorted(valuesUdpDict[k]):
            percentToAvg = math.fabs(valuesUdpDict[k][k1]-valuesUdpDict['average'][k1])*100/valuesUdpDict['average'][k1]
            percentToRate = math.fabs(valuesUdpDict[k][k1]-k1)*100/k1
            
            problem = ""
            if percentToAvg > percentageAcceptance:
                problem = "**ERROR**"
            
            problem1 = ""
            maxRate = 0
            if percentToRate > percentageAcceptance:
                problem1 = "**ERROR**"
                maxRate = valuesUdpDict['average'][k1]
            
            problem2 = ""
            if percentToRate > percentageAcceptance and k1 < udpMaxRate:
                problem2 = "**ERROR**"
                #add to "outliers" 
                #if outliersDict.has_key(k):
                #    outliersDict[k]['udp'] = "**ATTENTION**"
                #else:
                #    outliersDict[k] = {'udp':"**ATTENTION**"}
                    
            if problem2 != '':    
                tempOut1 +=  " UDP  | " + appendSpaces(k,16) + " || " + formatNumAppendSpaces(k1,5,0) + " MB/s | " + formatNumAppendSpaces(valuesUdpDict['average'][k1],15) + " MB/s | " + formatNumAppendSpaces(valuesUdpDict[k][k1],12) + " MB/s || " + formatNumAppendSpaces(percentToAvg,18,2) + "% | " + formatNumAppendSpaces(percentToRate,25,2) + "%\n"
                tempOut1_html += "<tr><td>UDP</td><td>"+k+"</td><td>"+formatNumAppendSpaces(k1,5,0)+" MB/s</td><td>"+formatNumAppendSpaces(valuesUdpDict['average'][k1],15) + " MB/s</td><td>" + formatNumAppendSpaces(valuesUdpDict[k][k1],12) + " MB/s</td><td>"+formatNumAppendSpaces(percentToAvg,18,2)+"%</td><td>"+formatNumAppendSpaces(percentToRate,25,2)+"%</td></tr>"
            
            tempOut2 += " UDP  | " + appendSpaces(k,16) + " || " + formatNumAppendSpaces(k1,17,0) + "  | " + formatNumAppendSpaces(valuesUdpDict['average'][k1],27) + " || " + formatNumAppendSpaces(valuesUdpDict[k][k1],24) + " | " + formatNumAppendSpaces(percentToAvg,12,2) + "% | " + formatNumAppendSpaces(percentToRate,13,2) + "% | "+problem+" | " + problem1 + " | " +problem2 + "\n"
            tempOut2_html += "<tr><td>UDP</td><td>"+k+"</td><td>"+formatNumAppendSpaces(k1,17,0) + "  </td><td> " +  formatNumAppendSpaces(valuesUdpDict['average'][k1],27)+" </td><td>"+formatNumAppendSpaces(valuesUdpDict[k][k1],24) + "</td><td>" + formatNumAppendSpaces(percentToAvg,12,2) + " %</td><td>"+formatNumAppendSpaces(percentToRate,13,2)+"%</td><td>"+problem+"</td><td>"+problem1+"</td><td>"+problem2+"</td></tr>"

    for k in sorted(valuesTcpDict):
        for k1 in sorted(valuesTcpDict[k]):
            percentToAvg = math.fabs(valuesTcpDict[k][k1]-valuesTcpDict['average'][k1])*100/valuesTcpDict['average'][k1]
            percentToRate = math.fabs(valuesTcpDict[k][k1]-k1)*100/k1
            
            
            problem = ""
            if percentToAvg > percentageAcceptance:
                problem = "**ERROR**"
            
            problem1 = ""
            maxRate = 0
            if percentToRate > percentageAcceptance:
                problem1 = "**ERROR**"
                maxRate = valuesTcpDict['average'][k1]
            
            problem2 = ""
            if percentToRate > percentageAcceptance and k1 < tcpMaxRate:
                problem2 = "**ERROR**"
                 #add to "outliers" 
                #if outliersDict.has_key(k):
                #    outliersDict[k]['tcp'] = "**ATTENTION**"
                #else:
                #    outliersDict[k] = {'tcp':"**ATTENTION**"}
                
            if problem2 != '':    
                tempOut1 +=  " TCP  | " + appendSpaces(k,16) + " || " + formatNumAppendSpaces(k1,5,0) + " MB/s | " + formatNumAppendSpaces(valuesTcpDict['average'][k1],15) + " MB/s | " + formatNumAppendSpaces(valuesTcpDict[k][k1],12) + " MB/s || " + formatNumAppendSpaces(percentToRate,18,2) + "% | " + formatNumAppendSpaces(percentToAvg,25,2) + "%\n"
                tempOut1_html +=  "<tr><td>TCP</td><td>" + appendSpaces(k,16) + "</td><td>" + formatNumAppendSpaces(k1,5,0) + " MB/s </td><td>" + formatNumAppendSpaces(valuesTcpDict['average'][k1],15) + " MB/s </td><td> " + formatNumAppendSpaces(valuesTcpDict[k][k1],12) + " MB/s </td><td> " + formatNumAppendSpaces(percentToRate,18,2) + "% </td><td> " + formatNumAppendSpaces(percentToAvg,25,2) + "%</td></tr>"
                
            tempOut2 += " TCP  | " + appendSpaces(k,16) + " || " + formatNumAppendSpaces(k1,17,0) + " | " + formatNumAppendSpaces(valuesTcpDict['average'][k1],27) + " || " + formatNumAppendSpaces(valuesTcpDict[k][k1],24) + " | " + formatNumAppendSpaces(percentToAvg,12,2) + "% | " + formatNumAppendSpaces(percentToRate,13,2) + "% | "+problem+" | " + problem1 + " | " +problem2 + "\n"
            tempOut2_html += " <tr><td>TCP</td><td>" + appendSpaces(k,16) + " </td><td>" + formatNumAppendSpaces(k1,17,0) + "</td><td>"+ formatNumAppendSpaces(valuesTcpDict['average'][k1],27) + " </td><td> " + formatNumAppendSpaces(valuesTcpDict[k][k1],24) + " </td><td> " + formatNumAppendSpaces(percentToAvg,12,2) + "% </td><td> " + formatNumAppendSpaces(percentToRate,13,2) + "% </td><td> "+problem+" </td><td>" + problem1 + " </td><td> " +problem2 + "</td></tr>"
    
    
    if tempOut1 != "":
        pre = 'Specific tests that failed ( > '+str(percentageAcceptance)+'% deviation from rate limit or cluster average):\n\n'
        pre+= ' Type |        IP        || Rate Limit | Cluster Sent/Rec Avg | Node Sent/Rec Avg || % Diff Node to Rate | % Diff Node to Cluster Avg\n'
        pre+= '-------------------------------------------------------------------------------------------------------------------------------------\n' 
        tempOut1 = pre + tempOut1 
        tempOut1_html = 'Specific tests that failed ( > '+str(percentageAcceptance)+'% deviation from rate limit or cluster average):<br/><br/><table><tr><th>Type </th><th>IP</th><th>Rate Limit</th><th>Cluster Sent/Rec Avg</th><th>Node Sent/Rec Avg</th><th>% Diff Node to Rate</th><th>% Diff Node to Cluster Avg</th></tr>'+tempOut1_html+'</table>'
        
    tempOut = 'Test        | Overall Cluster Performance\n'
    tempOut += '-----------------------------------------\n'
    tempOut += 'RTT Latency | '+ latencyPerf + ' ('+ str(latencyValue) + ' us)\n'
    tempOut += 'Clock Skew  | '+ clockSkewPerf + ' ('+ str(int(clockSkewValue)) + ' us)\n'
    tempOut += 'UDP         | '+ udpOverallPerf + ' (~' + str(int(round(udpMaxRate))) + ' MB/s)\n'
    tempOut += 'TCP         | '+ tcpOverallPerf + ' (~' + str(int(round(tcpMaxRate))) + ' MB/s)\n'
    tempOut += '\n\n'  #+ tempOut2 + "\n\n"
   
    tempOut_html = '<table><tr><th>Test</th><th>Overall Cluster Performance</th></tr>'
    tempOut_html += '<tr><td>RTT Latency</td><td>'+latencyPerf + ' ('+ str(latencyValue) + ' us)</td></tr>'
    tempOut_html += '<tr><td>Clock Skew</td><td>'+clockSkewPerf + ' ('+ str(int(clockSkewValue)) + ' us)</td></tr>'
    tempOut_html += '<tr><td>UDP</td><td>'+udpOverallPerf + ' (~'+ str(int(round(udpMaxRate))) + ' MB/s)</td></tr>'
    tempOut_html += '<tr><td>TCP</td><td>'+tcpOverallPerf + ' (~'+ str(int(round(tcpMaxRate))) + ' MB/s)</td></tr></table>'
    
    if len(outliersDict) > 0:
        tempOut += "The following nodes need attention (asterisks (**) indicate poor values):\n\n"
        tempOut += "Node             | RTT Latency Perf          | Clock Skew Perf\n"#   | UDP Perf      | TCP Perf\n"
        tempOut += "--------------------------------------------------------------\n"#---------------------------------\n"
        
        tempOut_html += "The following nodes need attention (asterisks (**) indicate poor values):<br/><br/>"
        tempOut_html += "<table><tr><th>Node</th><th>RTT Latency Perf</th><th>Clock Skew Perf</th></tr>"
        
        for k in sorted(outliersDict):
            skew = latency = tcp = udp = "-"
            if outliersDict[k].has_key('skew'):
                skew = outliersDict[k]['skew']
            if outliersDict[k].has_key('latency'):
                latency = outliersDict[k]['latency']
            #if outliersDict[k].has_key('tcp'):
            #    tcp = outliersDict[k]['tcp']
            #if outliersDict[k].has_key('udp'):
            #    udp = outliersDict[k]['udp']
            tempOut += appendSpaces(k,16) + " | " + appendSpaces(latency,25) + " | " + appendSpaces(skew,12) +"\n"#+ " | " + appendSpaces(udp,13) + " | " + appendSpaces(tcp,13) +"\n"
            tempOut_html += '<tr><td>'+k+'</td><td>'+latency+'</td><td>'+skew+'</td></tr>'
        tempOut_html += '</table>'
    if tempOut1 != "":
        tempOut += "\n\n"+tempOut1
        tempOut_html += '<br/><br/>' + tempOut1_html

    tcpEvalMatrix = {'POOR': 100, 'MARGINAL': 400, 'GOOD': 800}
    udpEvalMatrix = {'POOR': 100, 'MARGINAL': 200, 'GOOD': 400}
    
    
    refMatrix = 'Reference matrix of acceptable values:\n\n'
    refMatrix = refMatrix + '         | Latency (us) | Clock Skew (s) | UDP (MB/s) | TCP (MB/s)\n'
    refMatrix = refMatrix + '-------------------------------------------------------------------\n'
    refMatrix = refMatrix + 'HIGH     |       <'+str(latencyEvalMatrix['GOOD'])+'   |        <'+formatNumAppendSpaces(clockSkewEvalMatrix['GOOD']*1.0/1000000,3)+'    |      >' + str(udpEvalMatrix['GOOD'])+'  |     >' + str(tcpEvalMatrix['GOOD']) + '\n'
    refMatrix = refMatrix + 'GOOD     |    '+str(latencyEvalMatrix['GOOD'])+'-'+str(latencyEvalMatrix['MARGINAL'])+'   |     '+formatNumAppendSpaces(clockSkewEvalMatrix['GOOD']*1.0/1000000,3)+'-'+formatNumAppendSpaces(clockSkewEvalMatrix['MARGINAL']*1.0/1000000,3)+'    |   '+str(udpEvalMatrix['MARGINAL']) + '-'+str(udpEvalMatrix['GOOD']) + '  |  '+str(tcpEvalMatrix['MARGINAL']) + '-'+str(tcpEvalMatrix['GOOD']) + '\n'
    refMatrix = refMatrix + 'MARGINAL |    '+str(latencyEvalMatrix['MARGINAL'])+'-'+str(latencyEvalMatrix['POOR'])+'   |     '+formatNumAppendSpaces(clockSkewEvalMatrix['MARGINAL']*1.0/1000000,3)+'-'+formatNumAppendSpaces(clockSkewEvalMatrix['POOR']*1.0/1000000,3)+'    |   '+str(udpEvalMatrix['POOR']) + '-'+str(udpEvalMatrix['MARGINAL']) + '  |  '+str(tcpEvalMatrix['POOR']) + '-'+str(tcpEvalMatrix['MARGINAL']) + '\n'
    refMatrix = refMatrix + 'POOR     |       >'+str(latencyEvalMatrix['POOR'])+'   |        >'+formatNumAppendSpaces(clockSkewEvalMatrix['POOR']*1.0/1000000,3)+'    |      <' + str(udpEvalMatrix['POOR'])+'  |     <' + str(tcpEvalMatrix['POOR']) + '\n'
    
    refMatrix_html = 'Reference matrix of acceptable values:<br/><br/>'
    refMatrix_html = refMatrix_html + '<table><tr><th></th><th>Latency (us)</th><th>Clock Skew (s)</th><th>UDP (MB/s)</th><th>TCP (MB/s)</th></tr>'
    refMatrix_html = refMatrix_html + '<tr><td>HIGH</td><td> <'+str(latencyEvalMatrix['GOOD'])+' </td><td> <'+formatNumAppendSpaces(clockSkewEvalMatrix['GOOD']*1.0/1000000,3)+'</td><td> >' + str(udpEvalMatrix['GOOD'])+'</td><td> >' + str(tcpEvalMatrix['GOOD']) + '</td></tr>'
    refMatrix_html = refMatrix_html + '<tr><td>GOOD</td><td>  '+str(latencyEvalMatrix['GOOD'])+'-'+str(latencyEvalMatrix['MARGINAL'])+'</td><td>'+formatNumAppendSpaces(clockSkewEvalMatrix['GOOD']*1.0/1000000,3)+'-'+formatNumAppendSpaces(clockSkewEvalMatrix['MARGINAL']*1.0/1000000,3)+'</td><td>'+str(udpEvalMatrix['MARGINAL']) + '-'+str(udpEvalMatrix['GOOD']) + '</td><td>'+str(tcpEvalMatrix['MARGINAL']) + '-'+str(tcpEvalMatrix['GOOD']) + '</td></tr>'
    refMatrix_html = refMatrix_html + '<tr><td>MARGINAL</td><td>'+str(latencyEvalMatrix['MARGINAL'])+'-'+str(latencyEvalMatrix['POOR'])+'</td><td>'+formatNumAppendSpaces(clockSkewEvalMatrix['MARGINAL']*1.0/1000000,3)+'-'+formatNumAppendSpaces(clockSkewEvalMatrix['POOR']*1.0/1000000,3)+'</td><td>'+str(udpEvalMatrix['POOR']) + '-'+str(udpEvalMatrix['MARGINAL']) + '</td><td>'+str(tcpEvalMatrix['POOR']) + '-'+str(tcpEvalMatrix['MARGINAL']) + '</td></tr>'
    refMatrix_html = refMatrix_html + '<tr><td>POOR </td><td> >'+str(latencyEvalMatrix['POOR'])+'</td><td> >'+formatNumAppendSpaces(clockSkewEvalMatrix['POOR']*1.0/1000000,3)+' </td><td> <' + str(udpEvalMatrix['POOR'])+'</td><td> <' + str(tcpEvalMatrix['POOR']) + '</td></tr></table>'

    cleanOutput = cleanOutput + tempOut
    cleanOutput_html = cleanOutput_html + tempOut_html
    
    return cleanOutput, refMatrix, cleanOutput_html, refMatrix_html

# function used to parse and evaluate VIOPERF results    
def parseVioperfResult(output):
    writeEvalMatrix = {'POOR': 30, 'MARGINAL': 50, 'DECENT': 70, 'GOOD': 100}
    reWriteEvalMatrix = {'POOR': 15, 'MARGINAL': 20, 'DECENT': 30, 'GOOD': 50}
    readEvalMatrix = {'POOR': 20, 'MARGINAL': 40, 'DECENT': 60, 'GOOD': 70}
    skipEvalMatrix = {'POOR': 50, 'MARGINAL': 100, 'DECENT': 120, 'GOOD': 150}
 
    writeSamples = {}
    reWriteSamples = {}
    readSamples = {}
    skipSamples = {}
    nodeIPs = list()
    
    reWrite = re.compile('Write\s+\|\s+(.+?)\s+\|\s+MB/s\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)')
    reReWrite = re.compile('ReWrite\s+\|\s+(.+?)\s+\|\s+\(MB\-read\+MB\-write\)/s\s*\|\s+(\d+\.?\d*\+\d+\.?\d*)\s+\|\s+(\d+\.?\d*\+\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\+\d+\.?\d*\s+\|\s+(\d+\.?\d*)\+\d+\.?\d*\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)')
    reRead = re.compile('Read\s+\|\s+(.+?)\s+\|\s+MB/s\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)')
    reSkip = re.compile('SkipRead\s+\|\s+(.+?)\s+\|\s+seeks/s\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)\s+\|\s+(\d+\.?\d*)')
    reNode = re.compile('VIOPERF RESULTS FOR (.+)')
   
    nodeCount = 0
    currentNode = ''
    for line in output.split('\n'):
        reMatch = reNode.match(line)
        if (reMatch):
            value = reMatch.group(1)
            nodeIPs.append(value)
            currentNode = value
            nodeCount += 1
            continue
            
        reMatch = reWrite.match(line)
        if (reMatch):
            value = float(reMatch.group(5))
            if not writeSamples.has_key(currentNode):
                writeSamples[currentNode] = list()
            writeSamples[currentNode].append(value)
            writeThreads = int(reMatch.group(6))
            continue
        
        reMatch = reReWrite.match(line)
        if (reMatch):
            value = float(reMatch.group(5))
            if not reWriteSamples.has_key(currentNode):
                reWriteSamples[currentNode] = list()
            reWriteSamples[currentNode].append(value)
            reWriteThreads = int(reMatch.group(6))
            continue

        reMatch = reRead.match(line)
        if (reMatch):
            value = float(reMatch.group(5))
            if not readSamples.has_key(currentNode):
                readSamples[currentNode] = list()
            readSamples[currentNode].append(value)
            readThreads = int(reMatch.group(6))
            continue

        reMatch = reSkip.match(line)
        if (reMatch):
            value = float(reMatch.group(5))
            if not skipSamples.has_key(currentNode):
                skipSamples[currentNode] = list()
            skipSamples[currentNode].append(value)
            skipThreads = int(reMatch.group(6))
            continue 
            
    cleanOutput = "The vioperf tool was run in all "+str(nodeCount)+" nodes simultaneously on the respective data\nfolder. Your I/O benchmark results per thread, averaged across the entire\ncluster, are summarized below (Detailed test results available in the report):\n\n"
    cleanOutput_html = "The vioperf tool was run in all "+str(nodeCount)+" nodes simultaneously on the respective data folder. Your I/O benchmark results per thread, averaged across the entire\ncluster, are summarized below (Detailed test results available in the report):<br/><br/>"
    
    minDict = {}
    for k,v in writeSamples.items():
        v.sort()
        minDict[k] = [v[2],-1,-1,-1]
    for k,v in reWriteSamples.items():
        v.sort()
        minDict[k] = [minDict[k][0],v[2],minDict[k][2],minDict[k][3]]
    for k,v in readSamples.items():
        v.sort()
        minDict[k] = [minDict[k][0],minDict[k][1],v[2],minDict[k][3]]
    for k,v in skipSamples.items():
        v.sort()
        minDict[k] = [minDict[k][0],minDict[k][1],minDict[k][2],v[2]]
       
    (writeMin, writeMax, writeMean) = calcMinMaxMeanIo(writeSamples)
    (reWriteMin, reWriteMax, reWriteMean) = calcMinMaxMeanIo(reWriteSamples)
    (readMin, readMax, readMean) = calcMinMaxMeanIo(readSamples)
    (skipMin, skipMax, skipMean) = calcMinMaxMeanIo(skipSamples)
    
    #find outliers
    outliers = ""
    outliers_html = ""
    for i in range (0,len(nodeIPs)):
        node = nodeIPs[i]
        mins = minDict[node]
        perfResults = calcPerf(mins, [writeEvalMatrix,reWriteEvalMatrix,readEvalMatrix,skipEvalMatrix], ['POOR','MARGINAL','DECENT','GOOD','HIGH'])
        if('**POOR**' in perfResults or '**MARGINAL**' in perfResults):
            outliers = outliers + appendSpaces(node.strip(),15) + " | " + appendSpaces(perfResults[0],12) + " ("+formatNumAppendSpaces(mins[0],5)+" MB/s) | " + appendSpaces(perfResults[1],12) +  " ("+formatNumAppendSpaces(mins[1],5)+" MB/s) | " + appendSpaces(perfResults[2],12) + " ("+formatNumAppendSpaces(mins[2],5)+" MB/s) | " + appendSpaces(perfResults[3],12) + " ("+formatNumAppendSpaces(mins[3],5)+" seeks/s)\n"
            outliers_html += '<tr><td>'+node.strip()+'</td><td>'+perfResults[0]+ " ("+formatNumAppendSpaces(mins[0],5)+" MB/s)</td><td>" + perfResults[1] + " ("+formatNumAppendSpaces(mins[1],5)+" MB/s)</td><td>" + perfResults[2] + " ("+formatNumAppendSpaces(mins[2],5)+" MB/s)</td><td>" + perfResults[3] + " ("+formatNumAppendSpaces(mins[3],5)+" seeks/s)</td></tr>"
    
    if outliers != "":
        tempOutliers = appendSpaces("",15) + " | Write                     | ReWrite                   | Read                      | SkipRead\n"
        tempOutliers += "-----------------------------------------------------------------------------------------------------------------------------------\n" 
        outliers = tempOutliers + outliers
        outliers_html = '<table><tr><th></th><th>Write</th><th>ReWrite</th><th>Read</th><th>SkipRead</th></tr>'+outliers_html+'</table>'
        
    #overal results
    mins = [writeMin, reWriteMin, readMin, skipMin]
    perfResults = calcPerf(mins, [writeEvalMatrix,reWriteEvalMatrix,readEvalMatrix,skipEvalMatrix], ['POOR','MARGINAL','DECENT','GOOD','HIGH'])
        
    tempOut = 'Test     | Cluster Average/Thread | Cluster Min Performance Detected\n'
    tempOut = tempOut + '------------------------------------------------------------------------\n' 
    tempOut = tempOut + 'Write    | ' + formatNumAppendSpaces(writeMean, 17) + ' MB/s | ' + appendSpaces(perfResults[0],12) + ' ('+str(writeMin)+' MB/s)\n'
    tempOut = tempOut + 'ReWrite  | ' + formatNumAppendSpaces(reWriteMean, 17) + ' MB/s | ' + appendSpaces(perfResults[1],12) + ' ('+str(reWriteMin)+' MB/s)\n'
    tempOut = tempOut + 'Read     | ' + formatNumAppendSpaces(readMean, 17) + ' MB/s | ' + appendSpaces(perfResults[2],12) + ' ('+str(readMin)+' MB/s)\n'
    tempOut = tempOut + 'SkipRead | ' + formatNumAppendSpaces(skipMean, 14) + ' seeks/s | ' + appendSpaces(perfResults[3],12) + ' ('+str(skipMin)+' seeks/s)\n'
    
    tempOut_html = '<table><tr><th>Test</th><th>Cluster Average/Thread</th><th>Cluster Min Performance Detected</th></tr>'
    tempOut_html += '<tr><td>Write</td><td>' + formatNumAppendSpaces(writeMean, 17) + ' MB/s</td><td>' + appendSpaces(perfResults[0],12) + ' ('+str(writeMin)+' MB/s)</td></tr>'
    tempOut_html += '<tr><td>ReWrite</td><td>' + formatNumAppendSpaces(reWriteMean, 17) + ' MB/s</td><td>' + appendSpaces(perfResults[1],12) + ' ('+str(reWriteMin)+' MB/s)</td></tr>'
    tempOut_html += '<tr><td>Read</td><td>' + formatNumAppendSpaces(readMean, 17) + ' MB/s</td><td>' + appendSpaces(perfResults[2],12) + ' ('+str(readMin)+' MB/s)</td></tr>'
    tempOut_html += '<tr><td>SkipRead</td><td>' + formatNumAppendSpaces(skipMean, 14) + ' seeks/s</td><td>' + appendSpaces(perfResults[3],12) + ' ('+str(skipMin)+' seeks/s)</td></tr></table>'
    
    if(outliers != ""):
        tempOut = tempOut + "\nThe following nodes need attention (asterisks (**) indicate poor values):\n\n" + outliers
        tempOut_html += "<br/>The following nodes need attention (asterisks (**) indicate poor values):<br/><br/>" + outliers_html
   
    refMatrix = 'Reference matrix of acceptable values:\n\n'
    refMatrix = refMatrix + '         | Write  | ReWrite |  Read | SkipRead\n'
    refMatrix = refMatrix + '----------------------------------------------\n'
    refMatrix = refMatrix + 'HIGH     |   >'+str(writeEvalMatrix['GOOD'])+' |    >'+str(reWriteEvalMatrix['GOOD'])+'  |   >'+str(readEvalMatrix['GOOD'])+' |    >'+str(skipEvalMatrix['GOOD'])+'\n'
    refMatrix = refMatrix + 'GOOD     | '+str(writeEvalMatrix['DECENT'])+'-'+str(writeEvalMatrix['GOOD'])+' |  '+str(reWriteEvalMatrix['DECENT'])+'-'+str(reWriteEvalMatrix['GOOD'])+'  | '+str(readEvalMatrix['DECENT'])+'-'+str(readEvalMatrix['GOOD'])+' | '+str(skipEvalMatrix['DECENT'])+'-'+str(skipEvalMatrix['GOOD'])+'\n'
    refMatrix = refMatrix + 'DECENT   | '+str(writeEvalMatrix['MARGINAL'])+'-'+str(writeEvalMatrix['DECENT'])+'  |  '+str(reWriteEvalMatrix['MARGINAL'])+'-'+str(reWriteEvalMatrix['DECENT'])+'  | '+str(readEvalMatrix['MARGINAL'])+'-'+str(readEvalMatrix['DECENT'])+' | '+str(skipEvalMatrix['MARGINAL'])+'-'+str(skipEvalMatrix['DECENT'])+'\n'
    refMatrix = refMatrix + 'MARGINAL | '+str(writeEvalMatrix['POOR'])+'-'+str(writeEvalMatrix['MARGINAL'])+'  |  '+str(reWriteEvalMatrix['POOR'])+'-'+str(reWriteEvalMatrix['MARGINAL'])+'  | '+str(readEvalMatrix['POOR'])+'-'+str(readEvalMatrix['MARGINAL'])+' |  '+str(skipEvalMatrix['POOR'])+'-'+str(skipEvalMatrix['MARGINAL'])+'\n'
    refMatrix = refMatrix + 'POOR     |   <'+str(writeEvalMatrix['POOR'])+'  |    <'+str(reWriteEvalMatrix['POOR'])+'  |   <'+str(readEvalMatrix['POOR'])+' |    <'+str(skipEvalMatrix['POOR'])+'\n'
    
    refMatrix_html = 'Reference matrix of acceptable values:<br/><br/>'
    refMatrix_html +=  '<table><tr><th></th><th>Write</th><th>ReWrite</th><th>Read</th><th>SkipRead</th></tr>'
    refMatrix_html +=  '<tr><td>HIGH    </td><td>  >'+str(writeEvalMatrix['GOOD'])+' </td><td>   >'+str(reWriteEvalMatrix['GOOD'])+'</td><td>  >'+str(readEvalMatrix['GOOD'])+'</td><td>  >'+str(skipEvalMatrix['GOOD'])+'</td></tr>'
    refMatrix_html +=  '<tr><td>GOOD    </td><td>'+str(writeEvalMatrix['DECENT'])+'-'+str(writeEvalMatrix['GOOD'])+'</td><td> '+str(reWriteEvalMatrix['DECENT'])+'-'+str(reWriteEvalMatrix['GOOD'])+' </td><td>'+str(readEvalMatrix['DECENT'])+'-'+str(readEvalMatrix['GOOD'])+'</td><td>'+str(skipEvalMatrix['DECENT'])+'-'+str(skipEvalMatrix['GOOD'])+'</td></tr>'
    refMatrix_html +=  '<tr><td>DECENT  </td><td>'+str(writeEvalMatrix['MARGINAL'])+'-'+str(writeEvalMatrix['DECENT'])+' </td><td> '+str(reWriteEvalMatrix['MARGINAL'])+'-'+str(reWriteEvalMatrix['DECENT'])+' </td><td>'+str(readEvalMatrix['MARGINAL'])+'-'+str(readEvalMatrix['DECENT'])+'</td><td>'+str(skipEvalMatrix['MARGINAL'])+'-'+str(skipEvalMatrix['DECENT'])+'</td></tr>'
    refMatrix_html +=  '<tr><td>MARGINAL</td><td>'+str(writeEvalMatrix['POOR'])+'-'+str(writeEvalMatrix['MARGINAL'])+' </td><td> '+str(reWriteEvalMatrix['POOR'])+'-'+str(reWriteEvalMatrix['MARGINAL'])+' </td><td>'+str(readEvalMatrix['POOR'])+'-'+str(readEvalMatrix['MARGINAL'])+'</td><td> '+str(skipEvalMatrix['POOR'])+'-'+str(skipEvalMatrix['MARGINAL'])+'</td></tr>'
    refMatrix_html +=  '<tr><td>POOR    </td><td>  <'+str(writeEvalMatrix['POOR'])+' </td><td>   <'+str(reWriteEvalMatrix['POOR'])+' </td><td>  <'+str(readEvalMatrix['POOR'])+'</td><td>   <'+str(skipEvalMatrix['POOR'])+'</td></tr></table>'
    
    cleanOutput = cleanOutput + tempOut
    cleanOutput_html += tempOut_html
    
    return cleanOutput, refMatrix, cleanOutput_html, refMatrix_html

# function used to parse and evaluate VCPUPERF results
def parseVcpuperfResult(output):
    
    scalingThreshold = 100
    realTimeEvalMatrix = {'POOR': 12, 'MEDIUM': 8}
    
    loadMaxDifferencePercent = 0.10
    
    cpuTimeSamples = list()
    realTimeSamples = list()
    highLoadSamples = list()
    lowLoadSamples = list()
    nodeIPs = list()
    
    reRealTime = re.compile('\s*Real Time:\s*(.*?)s')
    reHighLoad = re.compile("This machine's high load time: (\d+) microseconds.")
    reLowLoad = re.compile("This machine's low load time: (\d+) microseconds.")
    reNode = re.compile('VCPUPERF RESULTS FOR (.+)')
   
    nodeCount = 0
    currentNode = ''
    resultsDict = {}
    for line in output.split('\n'):
        reMatch = reNode.match(line)
        if (reMatch):
            value = reMatch.group(1)
            nodeIPs.append(value)
            currentNode = value
            nodeCount += 1
            continue
            
        reMatch = reHighLoad.match(line)
        if (reMatch):
            value = int(reMatch.group(1))
            highLoadSamples.append(value)
            if not resultsDict.has_key(currentNode):
                resultsDict[currentNode] = [list(),list(),list()]
            resultsDict[currentNode][0].append(value)
            continue
 
        reMatch = reLowLoad.match(line)
        if (reMatch):
            value = int(reMatch.group(1))
            lowLoadSamples.append(value)
            if not resultsDict.has_key(currentNode):
                resultsDict[currentNode] = [list(),list(),list()]
            resultsDict[currentNode][1].append(value)
            continue

        reMatch = reRealTime.match(line)
        if (reMatch):
            value = float(reMatch.group(1))
            realTimeSamples.append(value)
            if not resultsDict.has_key(currentNode):
                resultsDict[currentNode] = [list(),list(),list()]
            resultsDict[currentNode][2].append(value)
            continue
            
    cleanOutput = "Your CPU benchmark results based on an average of "+str(len(resultsDict[currentNode][0]))+" runs on all "+str(nodeCount)+" nodes\n(Detailed test results available in the report):\n\n"
    cleanOutput_html = cleanOutput+"<br/><br/>"
    
    overallPerf = "HIGH"
    overallScaling = "OFF"
    outliers = ""
    outliers_html = ""
    
    realTimeMeans = list()
    for i in range (0,len(nodeIPs)):
        node = nodeIPs[i]
        highLoadSamples = resultsDict[node][0]
        lowLoadSamples = resultsDict[node][1]
        realTimeSamples = resultsDict[node][2]
        
        (realTimeMin, realTimeMax, realTimeMean) = calcMinMaxMean(realTimeSamples)
        (highLoadMin, highLoadMax, highLoadMean) = calcMinMaxMean(highLoadSamples)
        (lowLoadMin, lowLoadMax, lowLoadMean) = calcMinMaxMean(lowLoadSamples)
        realTimeMeans.append(realTimeMean)
        
        nodePerf = "HIGH"
        if(realTimeMean > realTimeEvalMatrix['POOR']):
            overallPerf = nodePerf = "**POOR**"
        elif(realTimeMean > realTimeEvalMatrix['MEDIUM']):
            nodePerf = "**MEDIUM**"
            if overallPerf != "**POOR**":
                overallPerf = nodePerf
        
        scaling = "OFF"
        if math.fabs(highLoadMean - lowLoadMean) > 100:
            overallScaling = scaling = "**ON** ("+str(math.fabs(highLoadMean - lowLoadMean))+" us high/low dif)"
            
        if "**ON**" in scaling or nodePerf == "**POOR**" or nodePerf == "**MEDIUM**":
            outliers = outliers + appendSpaces(node,15) + " | " + appendSpaces(nodePerf,15) + " | " + appendSpaces(scaling,6) + "\n"
            outliers_html = outliers_html +'<tr><td>'+ node + '</td><td>'+nodePerf+'</td><td>'+scaling+'</td></tr>'

    if outliers != "":
        outliers = appendSpaces("",15) + " | CPU performance | Scaling\n----------------------------------------------------------\n" + outliers
        outliers_html = '<table><tr><th></th><th>CPU performance</th><th>Scaling</th></tr>'+outliers_html+"</table>"

    a = 0
    for i in range(0,len(realTimeMeans)):
        a += realTimeMeans[i]
    realTimeMeanMean = a / len(realTimeMeans)
    
    tempOut = '          | cluster average value | performance \n'
    tempOut = tempOut + '-----------------------------------------------\n'
    tempOut = tempOut + 'real time | '+ formatNumAppendSpaces(realTimeMeanMean,19,2) + ' s | ' + overallPerf + ' \n\n' 
    tempOut_html='<table><tr><th></th><th>cluster average value</th><th>performance</th></tr><tr><td>real time</td><td>'+ formatNumAppendSpaces(realTimeMeanMean,10,2) + 's</td><td>'+overallPerf+'</td></tr></table><br/><br/>CPU scaling should be OFF on all nodes.<br/><br/>'
    tempOut = tempOut + 'CPU scaling should be OFF on all nodes.\n' 
    
    if overallScaling == "OFF":
        tempOut = tempOut + 'According to the tests CPU scaling is OFF on all nodes.\n' 
        tempOut_html = tempOut_html + 'According to the tests CPU scaling is OFF on all nodes.<br/>'
    else:
        tempOut = tempOut + 'According to the tests CPU scaling appears to be **ON** on one or more nodes.\n' 
        tempOut_html = tempOut_html + 'According to the tests CPU scaling appears to be **ON** on one or more nodes.<br/>' 
    
    refMatrix = 'Reference matrix of acceptable values:\n\n'
    refMatrix = refMatrix + '         | Real Time\n'
    refMatrix = refMatrix + '--------------------------\n'
    refMatrix = refMatrix + 'HIGH     |  < '+str(realTimeEvalMatrix['MEDIUM'])+' s\n'
    refMatrix = refMatrix + 'MEDIUM   | '+str(realTimeEvalMatrix['MEDIUM'])+'-'+str(realTimeEvalMatrix['POOR'])+' s\n'
    refMatrix = refMatrix + 'POOR     |  >'+str(realTimeEvalMatrix['POOR'])+' s\n'
    
    refMatrix_html = 'Reference matrix of acceptable values:<br/><br/><table><tr><th></th><th>Real Time</th></tr><tr><td>HIGH</td><td> <'+str(realTimeEvalMatrix['MEDIUM'])+' s</td></tr><tr><td>MEDIUM</td><td>'+str(realTimeEvalMatrix['MEDIUM'])+'-'+str(realTimeEvalMatrix['POOR'])+' s</td></tr><tr><td>POOR</td><td> >'+str(realTimeEvalMatrix['POOR'])+' s</td></tr></table>'
    
    if(outliers != ""):
        tempOut = tempOut + "\nThe following nodes need attention (asterisks (**) indicate poor values):\n\n" + outliers
        tempOut_html = tempOut_html + '<br/><br/>The following nodes need attention (asterisks (**) indicate poor values):<br/><br/>' + outliers_html
   
    cleanOutput = cleanOutput + tempOut
    cleanOutput_html = cleanOutput_html + tempOut_html
    return cleanOutput, refMatrix, cleanOutput_html, refMatrix_html

# helper function to get terminal size in order to truncate results correctly
def getTerminalSize():
    import os
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,'1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))
    return int(cr[1]), int(cr[0])
    
# function used to write to the HTML file
process_pid=None
def record_html(string,logFile):
   global process_pid
   if process_pid is None:
      process_pid = os.getpid()
              
   str = time.ctime()
   try:
       # Create log directory if it does not exist
       if not os.path.isdir(os.path.dirname(logFile)):
           os.mkdir(os.path.dirname(logFile))
   except IOError,ioe:
       print "Warning    : Can't write to '%s'"% logFile
       print "IOError    : %s " % ioe
       return

   except OSError,ioe:
       print "Warning    : Can't write to '%s'"% logFile
       print "OSError    : %s " % ioe
       print "Message was: %s " % string
       return

   if ( os.access( logFile, os.W_OK) or not os.path.isfile( logFile ) ):

       if not os.path.isfile(logFile):
           pre=html_pre+html_style
       else:
           pre=""
       log = None
       try:
           log = open( logFile, 'a' )
           log.write( "%s<div class='time'>%s [%s]</div> %s </br>" % (pre, str[4:20], process_pid, string) )
           log.flush()
       except IOError,ioe:
           print "Warning    : Can't write to '%s'"% logFile
           print "IOError    : %s " % ioe
           print "Message was: %s " % string
           return

       if log != None:
          log.close()

   else:
      print "WARNING: Can't write to %s" %  logFile

# function used to write to a log
def record(string, logFile = vBuddyLiteLog):
   global process_pid
   if process_pid is None:
      process_pid = os.getpid()
              
   str = time.ctime()

   try:
       # Create log directory if it does not exist
       if not os.path.isdir(os.path.dirname(logFile)):
           os.mkdir(os.path.dirname(logFile))
   except IOError,ioe:
       print "Warning    : Can't write to '%s'"% logFile
       print "IOError    : %s " % ioe
       return

   except OSError,ioe:
       print "Warning    : Can't write to '%s'"% logFile
       print "OSError    : %s " % ioe
       print "Message was: %s " % string
       return

   if ( os.access( logFile, os.W_OK) or
        not os.path.isfile( logFile ) ):

       log = None
       try:
           log = open( logFile, 'a' )
           log.write( "%s [%s] %s \n" % (str[4:20], process_pid, string) )
           log.flush()
       except IOError,ioe:
           print "Warning    : Can't write to '%s'"% logFile
           print "IOError    : %s " % ioe
           print "Message was: %s " % string
           return

       if log != None:
          log.close()

   else:
      print "WARNING: Can't write to %s" %  logFile
   
def DisableKeyboardInterrupt():
    # ignore the keyboard interrupte signal. 
    if threading.current_thread().name != 'MainThread':
        return

    return signal.signal(signal.SIGINT,signal.SIG_IGN)

def EnableKeyboardInterrupt( handler=signal.default_int_handler):
    # enable the keyboard interrupt with, potentially,  a custom
    # signal handler
    if threading.current_thread().name != 'MainThread':
        return

    return signal.signal(signal.SIGINT, handler)

# overwriting class adminController
class adminController:

    def runVnetperf(self, output_file):
        command = '/bin/bash -c \'/opt/vertica/bin/vnetperf --output-file /tmp/vnet.json > '+output_file+'\''
        record('[adminController.runVnetperf] Running command: ' + command)
        finalresult = os.system(command)
        return finalresult
        
    def runVcpuperf(self, output_file):
        database = self.check_database()
       
        if (database == -1):
            return

        sql = "-t@@SELECT /*+label(vBuddyLite)*/ node_address FROM nodes ORDER BY 1;"
        description = ""
        
        results = self.runSQLCommand(sql, description, database, False, False, False, True)
        
        nodelist = ""
        delim = ""
        for line in results.split("\n"):
            ip = line.strip()
            if ip!="":
                nodelist += delim + ip
                delim = ","
            
        temp_output = os.path.expanduser("~")+"/output"
     
        timesToRun = 5
        redirect = ">"
        
        os.system(">~/output")
        for i in range(0, timesToRun):
            
            command = '/bin/bash -c \'pids=(); nodelist=('+nodelist.replace(",", " ")+'); for ((i=0;i<${#nodelist[@]};i++)); do ssh -o StrictHostKeyChecking=no ${nodelist[i]} /opt/vertica/bin/vcpuperf -q '+redirect+' ~/output-${nodelist[i]} & pid="$! "; pids+=($pid); done; for i in ${pids[@]}; do wait ${i}; status=$?; echo $status >> ~/output; done;\''

            record('[adminController.runVcpuperf] Running iteration # '+str(i) + ' of command: ' + command)
        
            finalresult = os.system(command)
            redirect = ">>"
        
        FP = open(temp_output, "rb")
        output_wait = FP.read()
        FP.close()
        os.remove(temp_output)
        
        finalresult = 0
        for line in output_wait.split('\n'):
            record('[adminController.runVcpuperf] DEBUG wait output: ' + line.strip())
            if not (line.strip() == '' or line.strip() == '0'):
                finalresult = -1
        
        OUT = open(output_file, "w")
        for ip in nodelist.split(','):
            filename = os.path.expanduser("~")+'/output-'+ip
            FP = open(filename, "rb")
            output_cpu = FP.read()
            FP.close()
            os.remove(filename)
            OUT.write('VCPUPERF RESULTS FOR '+ip+'\n\n'+output_cpu + '\n\n')
        
        OUT.close()
        
        return finalresult
        
       
    def runVioperf(self, output_file, duration='60s', interval='2s'):
        database = self.check_database()
       
        if (database == -1):
            return

        sql = "-t@@SELECT /*+label(vBuddyLite)*/ node_address, storage_path FROM disk_storage NATURAL JOIN nodes WHERE storage_usage LIKE '%DATA%' ORDER BY 1;"
        description = ""
        
        results = self.runSQLCommand(sql, description, database, False, False, False, True)
        
        reNodeData = re.compile('\s*(.+?)\s+\|\s+(.+)')
        nodelist = ""
        data_dir = ""
        delim = ""
        for line in results.split("\n"):
            reMatch = reNodeData.match(line)
            if (reMatch):
                ip = reMatch.group(1)
                nodelist += delim + ip
                
                datadir = reMatch.group(2)
                reFolder = re.compile('(.+)/.+?')
                reMatch = reFolder.match(datadir)
                if(reMatch):
                    data_dir += delim + reMatch.group(1)
                else:
                    data_dir += delim + '"'+datadir+'"'
                delim = ","
            
        temp_output = os.path.expanduser("~")+"/output"
     
        command = '/bin/bash -c \'>~/output; pids=(); nodelist=('+nodelist.replace(",", " ")+'); datadirlist=('+data_dir.replace(",", " ")+'); for ((i=0;i<${#nodelist[@]};i++)); do ssh -o StrictHostKeyChecking=no ${nodelist[i]} /opt/vertica/bin/vioperf --log-interval='+interval+' --duration='+duration+' ${datadirlist[i]} > ~/output-${nodelist[i]} & pid="$! "; pids+=($pid); done; for i in ${pids[@]}; do wait ${i}; status=$?; echo $status >> ~/output; done;\''

        record('[adminController.runVioperf] Running command: ' + command)
        finalresult = os.system(command)
        
        FP = open(temp_output, "rb")
        output_wait = FP.read()
        FP.close()
        os.remove(temp_output)
        
        finalresult = 0
        for line in output_wait.split('\n'):
            record('[adminController.runVioperf] DEBUG wait output: ' + line.strip())
            if not (line.strip() == '' or line.strip() == '0'):
                finalresult = -1
        
        OUT = open(output_file, "w")
        for ip in nodelist.split(','):
            filename = os.path.expanduser("~")+'/output-'+ip
            FP = open(filename, "rb")
            output_io = FP.read()
            FP.close()
            os.remove(filename)
            OUT.write('VIOPERF RESULTS FOR '+ip+'\n\n'+output_io + '\n\n')
        
        OUT.close()
        
        return finalresult
        
    # Ctor: See the description of makeUniquePorts in adminExec
    def __init__(self, makeUniquePorts):
        self.__metaModel = adminMeta(self)
        self.__ui = uiMgr_vBuddyLite(self.__metaModel, titleString="Vertica Analytic Database %s vBuddy Lite %s" % (DBname.PRODUCT_VERSION, vBuddyLiteVersion))
        self.__exec = adminExec.adminExec(makeUniquePorts)
        self.__exec.setInteractive(self.__ui.getIsInteractive());
        self.DefaultCatalogDir = self.__exec.getDefaultCatalogDir()
        self.DefaultDataDir = self.__exec.getDefaultDataDir()
        self.component = ""
        self.DefaultDBDesignerDB = None # remember which database was used to design 
        self._logger = vertica.shared.logging.get_logger(self)

    def isOkToRun(self):
        return self.__exec.isOkToRun()

    def showMainHelp(self):
        self.__ui.infoBox(vBuddyLiteMainHelp)
        return
                
    def runSQLCommand(self, command, description, database, writeOutput=False, showOutput=True, exportDBDQueries=False, returnResultInsteadOfTime=False, graph=False, graphid=""):
        
        if ("Export Last Week's Queries for Database Designer" in description and not exportDBDQueries):
            record("[adminCtrl.runSQLCommand] '%s' query skipped" % description)
            return 0
            
        DBPassword = ""
        code = 0
        
        useDatabaseConnection = False
        for sql in command.split("###"):
            if('\\!' not in sql):
                useDatabaseConnection = True
        dbDict = self.__exec.getDBInfo()
        startInfo = dbDict["startinfo"][ database ]
                
        if not startInfo:
            record("[adminCtrl.runSQLCommand] dbDict error")                    
                        
        record("[adminCtrl.runSQLCommand] startInfo: %s" % startInfo)
        #host = startInfo[ 6 ][ 0 ] # 6 is for node list, 0 is for first host
        host = startInfo.nodeHosts[ 0 ]
                        
        binDir = DBinclude.binDir        
        
        portNo = self.__exec.getPortNo(database)
        debug_file = "/tmp/" + database + ".txt"
        cmd = binDir + "/vsql -p " + str(portNo) + " -h " + host + " " + "-d" + " " + database + " "
        
        if(EXPANDED):
            cmd += "-x "
            
        cmd += "-q " # no messages
        cmd += "-X " # skip ~/.vsqlrc
        #cmd += "-t " # remove column headers
            
        if(useDatabaseConnection):
            code, DBpassword = vsql.Passwords.Instance().getpass(database, self.__ui)
            
            
            if code:
                record("[adminCtrl.runSQLCommand] getpass returned %d" % (code))
                return 0
                     
            if DBpassword != '':
                DBpassword1 = vsql.escape_password(DBpassword)
                cmd += " -w " + DBpassword1 + " "

            errcode = os.WEXITSTATUS(os.system(cmd + "-c \" SELECT /*+label(vBuddyLite)*/ 1\" > " + debug_file))
            if errcode == 3:
                self.__ui.infoBox("Unable to connect to %s.\nSee log for details." % database)
                return 0

            if errcode == 2:
                vsql.Passwords.Instance().removePassFromCache(database)
                self.__ui.infoBox("Unable to connect to %s.\nVerify that the database is running  and\n your password is correct. " % (database))
                return 0
            if errcode == 1:
                self.__ui.infoBox("Connect error: %d" % (errcode,))
                return 0
        
        loadedresult = ""
        totalQueryTime = 0
        queryResult = ""
        content_html = ""

        if showOutput:
            self.__ui.infoBoxStay("\nCurrently running: \n\n'"+description+"'\n\nPlease wait...")
        
        max_chars_cut = 0
        for sql in command.split("###"):
            if sql.strip() == "":
                continue
            
            #see if there are any values we need to capture as input from the user, to execute the query
            for m in re.finditer(r'<<(.+?)>>',sql):
                variable = m.group(1)
                var_to_replace = m.group(0)
                help_text = "Help not available."
                #if(variable == "INTERNAL_stored_timestamp"):
                #    timestamp = self.runSQLCommand("-t@@SELECT /*+label(vBuddyLite)*/ NOW();", '', database, False, False, False, True).strip();
                #    sql = re.sub(var_to_replace, timestamp, sql)
                #    continue
                if(variable == "Scope"):
                    help_text = "The variable SCOPE can usually take the following values:\n\n  - an empty string (''). This will export all tables, or objects.\n  - '[dbName.][schema.]object'. This will export the specific object. Schemas, tables, vies, or projections are usually valid values.\n"
                code, answer = self.__ui.getInput(description+":\n"+re.sub(r'.','*',description)+"\n\nPlease type the '" + variable + "'", help_text)
                if (code == Navigator.NAV_CANCEL):
                    break
                sql = re.sub(var_to_replace,answer,sql)
            
            if (code == Navigator.NAV_CANCEL):
                    return
            moreDescription = ""
            if ('##' in sql):
                comments = sql.split('##')
                sql = comments.pop();
                moreDescription = '\n'.join(comments);
                    # sql = re.sub('##','\n',sql)
            cmd1 = ""
            if ('\\!' in sql):
                cmd1 = sql[2:] + " > " + debug_file
            else:
                extraParams = ''
                if ('@@' in sql):
                    extraParams = (sql.split('@@')[0]).strip() + ' '
                    sql = sql.split('@@')[1]
                cmd1 = cmd + extraParams + "-c \"" + sql + "\" > " + debug_file
            record("[adminCtrl.runSQLCommand] Connecting to " + database + " using command: " + cmd1)
            #self.__ui.clearScreen()
            
            startTime = time.time()
            if ('vcpuperf' in sql):
                finalresult = self.runVcpuperf(debug_file)
            elif ('vioperf' in sql):
                if('Quick' in description):
                    finalresult = self.runVioperf(debug_file, '60s')
                else:
                    finalresult = self.runVioperf(debug_file, '10min', '10s')
            elif ('vnetperf' in sql):
                finalresult = self.runVnetperf(debug_file)
            else:    
                finalresult = os.system(cmd1) 
            queryTime = time.time() - startTime
            totalQueryTime += queryTime
            
            content = None
            loadedresult += "\n\n" + moreDescription
            if('\\!' in sql or '\\?' in sql):
                loadedresult += "\n\nCOMMAND: " + sql[2:].lstrip()
            else:
                loadedresult += "\n\nQUERY: " + sql.lstrip()
            if finalresult == 0 or finalresult == 256:
                fP = open(debug_file, "rb")
                queryResult = content = fP.read()
                fP.close()
                
                if "vcpuperf" in sql:
                    (screenResults, evalMatrix, screenResults_html, evalMatrix_html) = parseVcpuperfResult(content)
                    if(writeOutput):
                        queryResult_html = content_html = screenResults_html +'<br/><br/>'+evalMatrix_html + '<br/><br/>Raw vcpuperf output:<br/><br/><pre>'+content.replace('\n','<br/>')+"</pre>"
                        queryResult = content = screenResults + "\n\n" + evalMatrix + "\n\nRaw vcpuperf output:\n\n"+content
                    else:
                        queryResult = content = screenResults
                if "vioperf" in sql:
                    (screenResults, evalMatrix, screenResults_html, evalMatrix_html) = parseVioperfResult(content)
                    if(writeOutput):
                        queryResult_html = content_html = screenResults_html +'<br/><br/>'+evalMatrix_html + '<br/><br/>Raw vioperf output:<br/><br/><pre>'+content.replace('\n','<br/>')+"</pre>"
                        queryResult = content = screenResults + "\n\n" + evalMatrix + "\n\nRaw vioperf output:\n\n"+content
                    else:
                        queryResult = content = screenResults
                if "vnetperf" in sql:
                    if finalresult == 256:
                        queryResult_html = content_html = "vnetperf could not run. Please note that vnetperf cannot be executed on single node clusters.<br/><br/>"
                        queryResult = content = "vnetperf could not run. Please note that vnetperf cannot be executed on single node clusters.\n\n"
                    else:
                        (screenResults, evalMatrix, screenResults_html, evalMatrix_html) = parseVnetperfResult(content)
                        if(writeOutput):
                            queryResult_html = content_html = screenResults_html +'<br/><br/>'+evalMatrix_html + '<br/><br/>Raw vnetperf output:<br/><br/><pre>'+content.replace('\n','<br/>')+"</pre>"
                            queryResult = content = screenResults + "\n\n" + evalMatrix + "\n\nRaw vnetperf output:\n\n"+content
                        else:
                            queryResult = content = screenResults
                    
                content = re.sub(r'^\s+EXPORT.+\n-+\n','',content)
                timing = str(math.floor(queryTime * (10 ** 2)) / (10 ** 2))+" s\n\n"
                if(queryTime < 1):
                    timing = str(math.floor(queryTime*1000 * (10 ** 2)) / (10 ** 2))+" ms\n\n"
                
                if (content.strip() != ''):
                    content += "\nCompleted in: "+ timing
                    if (content_html.strip() != ''):
                        content_html += '<br/>Completed in: '+timing+'<br/>'
                    
                #trim content if needed
                if showOutput:
                    if (content != None):
                        (tW, tH) = getTerminalSize()
                        (cW, cH) = self.__ui.getDim(content)
                        if(cW+7 > tW):
                            trimmed_content = ""
                            
                            for line in content.split('\n'):
                                if len(line) > (tW-7):
                                    trimmed_content = trimmed_content+"\n"+line[0:(tW-10)]+"..."
                                else:
                                    trimmed_content = trimmed_content+"\n"+line
                                if (cW-tW+7) > max_chars_cut:
                                    max_chars_cut = cW-tW+7
                                    
                            content = trimmed_content
                loadedresult += "\n\n" + content
            elif finalresult == 512:
                vsql.Passwords.Instance().removePassFromCache(database)
                loadedresult += "\n\nInvalid Database Password. Password Authentication Failed." 
            else:
                loadedresult += "\n\nError executing query. Error code = %d" % finalresult
            
            
        if ("Export Last Week's Queries for Database Designer" in description) and exportDBDQueries:
            b = re.compile(r'^.+?\n   1 \| ',re.DOTALL)
            logOutput = re.sub(b,'',loadedresult.strip()) 
            b = re.compile(r'\n   1 \| ')
            logOutput = re.sub(b,';\n\n',logOutput) 
            logOutput = re.sub(';;',';',logOutput) 
            b = re.compile(r';\n\(.+?rows\).+?$',re.DOTALL)
            logOutput = re.sub(b,';\n',logOutput) 
            
            log = None
            try:
                log = open( vBuddyLiteQueriesOutputLog, 'w' )
                log.write( "%s" % logOutput)
                log.flush()
            except IOError,ioe:
                print "Warning    : Can't write to '%s'"% vBuddyLiteQueriesOutputLog
                print "IOError    : %s " % ioe
                
            if log != None:
                log.close()
            
        elif writeOutput:
            logOutput = "\n\n============" + re.sub(r'.','=',description) + "\n#     "+description+"     #\n============"+re.sub(r'.','=',description)+"\n\n"+loadedresult.strip('\n')+"\n\n"
            record(logOutput, vBuddyLiteOutputLog)
            global javascript

            if content_html != "":
                if graph:
                    javascript += getJavascript(content_html, graphid)
                    content_html+="<div id='chart_div_"+graphid+"'></div>"
                record_html("<h3 id='"+description.split('. ')[0]+"'>"+description+"</h3><div>"+content_html+"<br/><a href='#top'>Top</a><br/></div>", vBuddyLiteOutputHtml)
            else:
                htmlified = ''
                extra=''
                if graph:
                    htmlified = htmlify('\n'+loadedresult.strip('\n'),True)
                    javascript += getJavascript(htmlified,graphid)
                    extra="<div id='chart_div_"+graphid+"'></div>"
                else:
                    htmlified = htmlify('\n'+loadedresult.strip('\n'),False)
                
                record_html("<h3 id='"+description.split('. ')[0]+"'>"+description+"</h3><div>"+htmlified+extra+"<br/><a href='#top'>Top</a><br/></div>", vBuddyLiteOutputHtml)
    
        if showOutput:        
            self.__ui.clearScreen()
            if ("Export Last Week's Queries for Database Designer" in description) and exportDBDQueries:
                self.__ui.infoBox("Queries for Database Designer were exported to:\n    %s" % vBuddyLiteQueriesOutputLog)
            else:
                try:
                    truncated_message = ""
                    if (loadedresult != None):
                        if max_chars_cut > 0:
                            truncated_message = "%d characters" % max_chars_cut
                        newContent = ""
                        lineCount = 0
                        for line in loadedresult.split('\n'):
                            if lineCount < 1000:
                                newContent += line + "\n"
                            else:
                                continue
                            lineCount += 1
                        
                        if lineCount > 1000:
                            if truncated_message == "":
                                truncated_message = "%d lines" % (lineCount - 1000)
                            else:
                                truncated_message = truncated_message + " and %d lines" % (lineCount - 1000)
                     
                        if not truncated_message == "":
                            
                            loadedresult = newContent + "\n\n(NOTE: results displayed were truncated by %s to fit the screen. CREATE FULL REPORT from the main menu to export the full output to a file)" % truncated_message
                    
                    self.__ui.infoBox("%s" % (description+"\n"+re.sub(r'.','*',description)+"\n\n" + loadedresult.strip('\n')))
                except:
                    self.__ui.infoBox("The results could not be displayed on the screen. Most likely the dimensions are too large. Try to export the Menu to a Report instead.")
        
        if returnResultInsteadOfTime:
            return queryResult
        
        return totalQueryTime
           
    def execute_menu_action(self):
    
        database = self.check_database()
       
        if (database == -1):
            return

        record("[adminController.execute_menu_action] Menu: "+VBUDDYLITE_MENU.replace("_"," ").strip() + ", item #: " + VBUDDYLITE_TAG);
        item = self.__metaModel.sqlQueries[VBUDDYLITE_MENU][int(VBUDDYLITE_TAG)-1]
        sql = ""
        description = ""
        # This is where we break down the command and decide how to run it
        # description|more description##sql/command
        if "|" in item:
            itemList = item.split("|")
            description = itemList.pop(0)
            sql = item.split("|",1)[1]
            record("[adminController.execute_menu_action] Running SQL Command for: "+description)
            if(description == "EXPORT ALL database DDLs (ENTIRE CATALOG) to a file"):
                if os.path.exists(vBuddyLiteOutputLog):
                    clearOutputLog = self.__ui.simpleYesNo("The output file %s already exists. \n\nWould you like to clear it before creating exporting the DDLs?" % vBuddyLiteOutputLog)
                    if clearOutputLog:
                        os.remove(vBuddyLiteOutputLog)
                    os.remove(vBuddyLiteOutputHtml)
                self.runSQLCommand(sql, description, database, writeOutput=True, showOutput=False)
                self.__ui.infoBox("Output was written to:\n    %s\nAn HTML version was written to:\n    %s" % (vBuddyLiteOutputLog, vBuddyLiteOutputHtml))
            else:
                description = VBUDDYLITE_MENU.split(".")[0][1:] + "." + VBUDDYLITE_TAG + ". " + description
                self.runSQLCommand(sql, description, database, exportDBDQueries=True)
        return

    def check_database(self):
        dbDict = self.__exec.getDBInfo()
        if (len((dbDict["defined"]).keys()) == 0):
            self.__ui.infoBox("No databases defined")
            return -1

        code, runningDBs = self.__exec.getRunningDatabases()

        if code:
            self.__ui.infoBox("Cannot connect to spread")
            return -1

        if runningDBs == []:
            self.__ui.infoBox("No databases are running")
            return -1

        #prepare an info structure the way the dialog thingie wants it
        items = []
        for db in runningDBs:
            item = (db, db, 0)
            items.append(item)
            
        # if there is only one database running and we know about it
        # (it is defined in dbDict), automatically connect
        if (len(items) == 1 and dbDict["defined"].has_key(items[0][0])):
            database = items[0][0]
        else:
            code, database = self.__ui.listBoxSelect("Select database to connect", items, runningDBs)
        if code == Navigator.NAV_CANCEL:
            return -1

        if not database:
            return -1
        
        # one final check (mostly for internal): if we don't know
        # about the database, don't try to connect to it
        if (database not in dbDict["defined"].keys()):
            self.__ui.infoBox("ERROR: nothing known about the database '%s'" % (database))
            return -1
        
        return database
    
    def execute_all_menu_actions(self):
        record("[adminController.execute_all_menu_actions] Report being created for all queries in '"+(VBUDDYLITE_MENU.replace("_"," ").strip())+"'.")
        
        database = self.check_database()
       
        if (database == -1):
            return
        
        menuDisplayName = " in '"+VBUDDYLITE_MENU.replace("_"," ").strip()+"'"
        if(VBUDDYLITE_MENU == "main"):
            menuDisplayName = ""
        showOutput = False
        writeOutput = True
        exportCatalog = False
        exportDBDQueries = False
        clearOutputLog = False
        includeVperf = False
        
        EXPORT_CATALOG = "Include DDLs of all objects in the database"
        CLEAR_OUTPUT = "Clear output file before creating new report"
        EXPORT_DBD = "Export last week's queries to a SQL file for DBD"
        INCLUDE_VPERF = "Include hardware tests (WARNING: these tests take about 15 minutes and stress the system)"
       
        options = []
        options.append([CLEAR_OUTPUT, "", "on"])
        if(VBUDDYLITE_MENU == "main"):
            options.append([EXPORT_CATALOG, "", "off"])
            options.append([EXPORT_DBD, "", "off"])
            options.append([INCLUDE_VPERF, "", "off"])
        if (VBUDDYLITE_MENU.replace("_"," ").strip()[-7:] == "Queries"):
            options.append([EXPORT_DBD, "", "on"])
        
        code, selections = self.__ui.multiListSelect("A report will be created for all queries" + menuDisplayName + " for database '"+database+"'.\n\nPlease select the desired options for the report:", options)
        if (code == 1):
            self.__ui.infoBox("Report generation was canceled.")
            return
        
        if(CLEAR_OUTPUT in selections):
            clearOutputLog = True
        if(EXPORT_CATALOG in selections):
            exportCatalog = True
        if(EXPORT_DBD in selections):
            exportDBDQueries = True
        if(INCLUDE_VPERF in selections):
            includeVperf = True
        
        if os.path.exists(vBuddyLiteOutputLog):
            if clearOutputLog:
                os.remove(vBuddyLiteOutputLog)
            os.remove(vBuddyLiteOutputHtml)
        
        menu_name = VBUDDYLITE_MENU
        if menu_name == "main":
            menu_name = "ALL TESTS"
        else:
            menu_name = "MENU '" + (VBUDDYLITE_MENU.replace("_"," ").strip())+"'"
            
        timeNow = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        line1 = "------------------------------------------------------------------------------------------"
        
        hostName = ""
        try:
            hostName = "(" + socket.gethostbyname(vBuddyLiteHostName) + ")"
        except:
            pass
        line2 = "     NEW REPORT - %s - DATABASE '%s'\n     CREATED BY %s ON %s %s @ %s UTC    " % (menu_name, database, vBuddyLiteLogUser, vBuddyLiteHostName, hostName, timeNow)
        record("\n\n"+line1+"\n"+line2+"\n"+line1+"\n\n", vBuddyLiteOutputLog)
        
        record_html("<div>"+vBuddyLiteVersion+"</div><h1 id='top'>"+line2+"</h1></head><body>", vBuddyLiteOutputHtml)
        self.__ui.clearScreen()
        self.__ui.gauge( "Progress: 0%", "Executing all queries" )
        # measure how long the report will take to create
        reportTime = 0
        
        if(VBUDDYLITE_MENU == "main"):
            items = sorted(self.__metaModel.sqlQueries.items(),key=lambda t: int(t[0].split('.')[0][1:]))
            
            contents_html = '<table class="contents"><tr><td><h2>Quick Navigation</h2><ul>'
            for k,v in items:
                if(k[-4:] == "DDLs"):
                    continue
                if("Hardware" in k and not includeVperf):
                    continue
                contents_html += '<li>'+k.split('.')[0].replace('_','')+'<a href="#'+k.split('.')[0].replace('_','')+'">'+k.split('.')[1]+'</a></li>'
                i = 1
                for q in v:
                    if "Hardware" in k and "Quick" in q:
                        i += 1
                        continue
                    if "Export Last Week" in q:
                        i += 1
                        continue
                    contents_html += '<li class="li2">'+k.split('.')[0].replace('_','')+'.'+str(i)+'<a href="#'+k.split('.')[0].replace('_','')+'.'+str(i)+'">'+q.split("|")[0]+'</a></li>'
                    i+=1
                
            if exportCatalog:
                contents_html += '<li><a href="#Full Database DDLs">Full Database DDLs</a></li>'
            contents_html += '</ul></td></tr></table>'
            
            if writeOutput:
                record_html(contents_html,vBuddyLiteOutputHtml)
            
            j = 1
            graph=False
            for k,v in items:
                if "Graphs" in k:
                    graph=True
                k = k.replace("_"," ").strip()
                record("[adminController.execute_all_menu_actions] Menu: "+k + ", item #: ALL");
                
                if(k[-4:] == "DDLs"):
                    #skip for report
                    continue
                if("Hardware" in k and not includeVperf):
                    continue
                    
                line1 = "############" + re.sub(r'.','#',k) + "\n"
                line2 = "#     "+k+"     #\n"
                line3 = "#     "+re.sub(r'.',' ',k)+"     #\n"
                
                if writeOutput:
                    record("\n\n"+line1+line3+line2+line3+line1+"\n\n", vBuddyLiteOutputLog)
                    record_html("<h2 id='"+k.split('.')[0]+"'>"+k+"</h2>",vBuddyLiteOutputHtml)

                i = 1
                prefix = k.split('.')[0]
                for q in v:
                    jj = j + i*1.0/len(v)
                    self.__ui.updateGauge( math.floor(jj*100/(len(items))), "Currently Running:\n    Menu:  '"+k+"'\n    Query: #"+str(i))
                
                    if "|" in q:
                        description = prefix + "." + str(i) + ". " + q.split("|")[0]
                        if "Hardware" in k and "Quick" in description:
                            i += 1
                            continue
                        
                        sql = q.split("|",1)[1]
                        record("[adminController.execute_all_menu_actions] Running SQL Command for: "+description)
                        reportTime += self.runSQLCommand(sql, description, database, writeOutput, showOutput, exportDBDQueries,False,graph,prefix.replace('_','') + "_" + str(i))
                    i = i+1
                j = j + 1
        else:
            menuName = VBUDDYLITE_MENU.replace("_"," ").strip()
            line1 = "############" + re.sub(r'.','#',menuName) + "\n"
            line2 = "#     "+menuName+"     #\n"
            line3 = "#     "+re.sub(r'.',' ',menuName)+"     #\n"
            
            prefix = menuName.split('.')[0]

            contents_html = '<table class="contents"><tr><td><h2>Quick Navigation</h2><ul>'
            contents_html += '<li>'+prefix.replace('_','')+'<a href="#'+prefix.replace('_','')+'">'+menuName.split('.')[1]+'</a></li>'
            i = 1
            for q in self.__metaModel.sqlQueries[VBUDDYLITE_MENU]:
                if "Hardware" in menuName and "Quick" in q:
                    i += 1
                    continue
                contents_html += '<li class="li2">'+prefix.replace('_','')+'.'+str(i)+'<a href="#'+prefix.replace('_','')+'.'+str(i)+'">'+q.split("|")[0]+'</a></li>'
                i+=1
           
            contents_html += '</ul></td></tr></table>'
            
            if writeOutput:
                record("\n\n"+line1+line3+line2+line3+line1+"\n\n", vBuddyLiteOutputLog)
                record_html(contents_html+"<h2 id='"+menuName.split('.')[0]+"'>"+menuName+"</h2>",vBuddyLiteOutputHtml)
                
            i = 1
            graph = False
            if 'Graphs' in menuName:
                graph= True

            for q in self.__metaModel.sqlQueries[VBUDDYLITE_MENU]:
                self.__ui.updateGauge( math.floor(i*100/len(self.__metaModel.sqlQueries[VBUDDYLITE_MENU])), "Currently Running:\n    Menu:  '"+menuName+"'\n    Query: #"+str(i))
                if "|" in q:
                    description = prefix + "." + str(i) + ". " + q.split("|")[0]
                    if "Hardware" in menuName and "Quick" in description:
                        i += 1
                        continue
                    sql = q.split("|",1)[1]
                    record("[adminController.execute_all_menu_actions] Running SQL Command for: "+description)
                    reportTime += self.runSQLCommand(sql, description, database, writeOutput, showOutput, exportDBDQueries,False,graph,prefix.replace('_','') + "_" + str(i))
                i = i+1
        
        if exportCatalog:
            reportTime += self.runSQLCommand("SELECT /*+label(vBuddyLite)*/ EXPORT_CATALOG();", "Full Database DDLs", database, writeOutput, showOutput)
        
        self.__ui.updateGauge( 100, "Tests Completed.")
        self.__ui.gaugeStop()
        
        global javascript
        if javascript != "":    
            javascript = '<script type="text/javascript">google.setOnLoadCallback(drawCurveTypes);function drawCurveTypes() {'+javascript+ ' } </script>'
        # record total time. 
        if writeOutput:
                record("\n\n== REPORT SUMMARY ==\n\nReport completed successfully.\nTotal execution time: "+str(math.floor(reportTime * (10 ** 2)) / (10 ** 2))+" s\n\n", vBuddyLiteOutputLog)
                record_html("<h4>REPORT SUMMARY</h4><div>Report completed successfully</br>Total Execution time: "+str(math.floor(reportTime * (10 ** 2)) / (10 ** 2))+" s</div>"+javascript+expandDataJS+"</body></html>",vBuddyLiteOutputHtml)
            
        if (exportDBDQueries):
            self.__ui.infoBox("Tests completed. Output was written to:\n    %s\nAn HTML version was written to:\n    %s\n\nQueries to be used in Database Designer were exported to:\n    %s" % (vBuddyLiteOutputLog,vBuddyLiteOutputHtml, vBuddyLiteQueriesOutputLog))
        else:
            self.__ui.infoBox("Tests completed. Output was written to:\n    %s\nAn HTML version was written to:\n    %s" % (vBuddyLiteOutputLog,vBuddyLiteOutputHtml))
        
        javascript = ''
                             
    def runApp(self):
        self.__ui.runFromNavigation("main")

# overwriting class adminMeta
class adminMeta:

    META_TITLE = 0
    META_ITEMS = 1
    META_ACTIONS = 2
    META_BACK = 3
    META_MENUPRESENT = 4
    META_ITEM_HELP = 5

    def buildMenu(self,text,menuName, parentMenuName,currentLevel):
        if(parentMenuName == None):
            record("[adminMeta.buildMenu] LEVEL: " + str(currentLevel) + ", PARENT: None, MENUNAME: " + menuName)
        else:
            record("[adminMeta.buildMenu] LEVEL: " + str(currentLevel) + ", PARENT: " + parentMenuName + ", MENUNAME: " + menuName)
            if parentMenuName == "Main Menu":
                parentMenuName = "main"
            else:
                parentMenuName = "_"+parentMenuName.replace(" ","_")
           
        if menuName == "Main Menu":
            menuName2 = "main"
        else:
            menuName2 = "_"+menuName.replace(" ","_")
            
        if(text.startswith("[")):
            #buildMenu
            if(currentLevel == 0):
                p = re.compile("^\[([^\[\]]+)\]",re.MULTILINE)
            elif (currentLevel == 1):
                p = re.compile("^\[\[([^\[\]]+)\]\]",re.MULTILINE)
            elif (currentLevel == 2):
                p = re.compile("^\[\[\[([^\[\]]+)\]\]\]",re.MULTILINE)
            
            menus1 = {}
            sectionStart = 0
            sectionEnd = 0
            menuName1 = ""
            for m in p.finditer(text):
                sectionEnd = m.start()-1
                if(menuName1):
                    menus1[menuName1] = text[sectionStart: sectionEnd]
                
                menuName1 = m.group(1)
                sectionStart = m.end()+1
            menus1[menuName1] = text[sectionStart:].strip()
        
            #build menu
            items = []
            actions = []
            item_help = []		
            for k,v in menus1.iteritems():
                items.append(k)
                actions.append("_"+k.replace(" ","_"))
                item_help.append("MENU '"+k+"'")
                self.buildMenu(v,k.strip(),menuName,(currentLevel+1))
            
            if(currentLevel == 0):
                items.append("CREATE FULL REPORT")
                actions.append(self.__controller.execute_all_menu_actions)
                item_help.append("\nCREATE FULL REPORT\n******************\n\nThis option will run all queries in all menus and save the output in:\n     %s\n\nIf selected to do so, the list of queries to be used in Database Designer will be exported to:\n     %s" % (vBuddyLiteOutputLog, vBuddyLiteQueriesOutputLog))
                
                items.append("Help Using vBuddy Lite")
                actions.append(self.__controller.showMainHelp)
                item_help.append("\nHelp Using vBuddy Lite\n**********************\n\nThis option will display the main help file for vBuddy Lite.\n")
            
            menu = [ menuName, items, actions, parentMenuName, menuName2, item_help ]
            self.__meta[ "menus" ][menuName2] = menu
            
        
        else:
            items = []
            actions = []
            item_help = []		
            text = text.strip()
            self.sqlQueries[menuName2] = []
            for line in text.splitlines():
                item = ""
                sql = ""
                if(line.strip() == ""):
                    continue
                if (line.startswith("//")):
                    continue
                if not "|" in line:
                    item = line
                    sql = ""
                else:
                    item = line.split("|")[0]
                    sql = line.split("|",1)[1]
                items.append(item)
                actions.append(self.__controller.execute_menu_action)
                self.sqlQueries[menuName2].append(line)
                if sql == "":
                    item_help.append("\n"+item+"\n"+re.sub(r'.','*',item)+"\n\n\nNo SQL query available.\n")
                else:
                    item_help.append("\n"+item+"\n"+re.sub(r'.','*',item)+"\n\n\nSQL QUERY or COMMAND:\n\n"+sql+"\n")   
                    
            if not menuName[-4:] == "DDLs":
                items.append("CREATE REPORT from All Tests In This Menu")
                actions.append(self.__controller.execute_all_menu_actions)
                item_help.append("\nCREATE REPORT from All Tests In This Menu\n*****************************************\n\nThis option will run all queries in the selected menu and save the output in %s" % vBuddyLiteLog)
        
            menu = [ menuName, items, actions, parentMenuName, menuName2, item_help ]          
            self.__meta[ "menus" ][menuName2] = menu
    
    def __defineMenus(self):
        
        self.__meta[ "menus" ] = {}
        
        menuFileContent = ""
        
        menuFileContent = sqlQueriesFile
        self.buildMenu(menuFileContent.strip(),"Main Menu",None,0)
        
    def __init__(self, controller):
        self.menuFile = "vBuddyLite.txt"
        self.sqlQueries = {}		
        self.__controller = controller
        self.__meta = {}
        self.__defineMenus()
     
    def getMenus(self):
        if "menus" in self.__meta.keys():
            return self.__meta[ "menus" ]
        else:
            return None
        
    def getpasswordPrompter(self, message):
        if "passwordprompter" in self.__meta.keys():
            if message in self.__meta[ "passwordprompter" ].keys():
                return self.__meta[ "passwordprompter" ][ message ]
            else:
                return self.__meta[ "passwordprompter" ][ "Enter the password for database" ]
        return None
        
    def getInputHelp(self, question):
        if "inputhelp" in self.__meta.keys():
            if question in self.__meta[ "inputhelp" ].keys():
                return self.__meta[ "inputhelp" ][ question ]
        return None
        
    def getlistBoxSelect(self, title):
        if "listboxselect" in self.__meta.keys():
            if title in self.__meta[ "listboxselect" ].keys():
                return self.__meta[ "listboxselect" ][ title ]
        return None 

    def getmultiListSelect(self, title):
        if "multilistselect" in self.__meta.keys():
            if title in self.__meta[ "multilistselect" ].keys():
                return self.__meta[ "multilistselect" ][ title ]
        return None 

# overwriting class uiMgr
class uiMgr_vBuddyLite:

    def __initializeMenuStructure(self, metaModel=None):

        if metaModel != None:
            menus = metaModel.getMenus()
            if menus != None:
                for menu in menus.keys():
                    items = menus[ menu ][ metaModel.META_ITEMS ]
                    actions = menus[ menu ][ metaModel.META_ACTIONS ]
                    back = menus[ menu ][ metaModel.META_BACK ]
                    menuPresent = menus[ menu ][ metaModel.META_MENUPRESENT ]
                    item_help = menus[ menu ][ metaModel.META_ITEM_HELP ]
                    self.__navigator.add_menu(menu, menus[ menu ][ metaModel.META_TITLE ], items, actions, back, menuPresent, item_help)

    # clears the screen (note the decoration so scripting works)
    @loginfo_noinput
    def clearScreen(self):
        os.system("clear")       

    @loginfo_noinput
    def infoBoxStay(self, info):
        w, h = self.getDim(info)
        (tW, tH) = getTerminalSize()
        if w > tW:
            h = h + 1 + ((w-tW)/tW)
        
        self.__dialog.infobox(info, height=h + 5, width=w + 7, no_collapse=True)

    @loginfo_noinput
    def infoBox(self, info):
        w, h = self.getDim(info)
        (tW, tH) = getTerminalSize()
        if w > tW:
            h = h + 1 + ((w-tW)/tW)
        
        self.__dialog.msgbox(info, height=h + 5, width=w + 7, no_collapse=True)

    @loginfo_noinput
    def textbox(self, info,title=None, exit=None, height=20, width=60):
        self.__dialog.textbox(info, height=height,width=width,title=title, exit=exit)
    
    def gauge(self, subtitle, gaugeName):
        self.__dialog.gauge_start(subtitle, title=gaugeName)

    def updateGauge(self, percent, updateText):
        display = updateText# % percent
        self.__dialog.gauge_update(percent, updateText, update_text=1)

    def gaugeStop(self):
       self.__dialog.gauge_stop()

    @loginteraction
    def listBoxSelect(self, title, info, answerKey):

        filename = self.__metaModel.getlistBoxSelect(title)
        code, answer = self.__navigator.getListChoice(title, info, filename)
        if code == Navigator.NAV_CANCEL:
            return code, ""
        if answer != None:
            if answer not in answerKey:
                if answer == "":
                    answer = "<empty>"
                self.infoBox("ERROR: Invalid selection: %s" % answer)
                return code, None
            return code, answer
    
    @loginteraction
    def multiListSelect(self, title, info, helpFile=None):
        code, answer = self.__navigator.getMultipleChoices(title, info, helpFile)
        return code, answer
        
    @loginteraction
    def getInput(self, question, help_text, defaultAnswer=""):
        
        code, result = self.__navigator.promptForInfo(question, help_text, defaultAnswer=defaultAnswer)
        return code, result
      
    @loginteraction
    def simpleYesNo(self, q, prompt=None, defaultYes=False):

        if q != None:
            if prompt != None:
                question = q % prompt
            else:
                question = q

            (w, h) = self.getDim(question)

            answer = not self.__dialog.yesno(question, width=w + 5, height=h + 5, defaultno=(not defaultYes))
            return answer
        else:
            print "questionName %s does not exit" % questionName
            sys.exit(1)
        return False
    
    @loginteraction
    def passwordPrompter(self, message=None):

        if message == None:
            prompt = "Password: "
        else:
            prompt = message
        filename = self.__metaModel.getpasswordPrompter(prompt) 
        code, result = self.__navigator.promptForPassword(prompt, filename)
        self.gauge("","")
        return code, result

    # returns the width and height of the supplied text
    def getDim(self, text, wrap=False):
        w = 0
        h = 0

        x = text.split("\n")
        h = len(x)

        for t in x:
            w = max(w, len(t))
            
            # only do this by request since we've used \n to make our 
            # messages fit, and it gets uglier for wide displays
            if (wrap and len(t) > 80):
                h += int(len(t) / 80)

        return (w, h)

    @loginfo
    def runFromNavigation(self, runFrom):
        self.__navigator.navigate(uimanager=self, startingPoint=runFrom)


    """
    callback: Navigator.navigate calls this method when a menu is to
    be displayed gives the uiMgr a chance to provided a scripted
    response or actually showing the menu via the Navigator
    """
    @loginteraction
    def showMenu(self, navigator, menu):
        # if scripting, return result, otherwise pass call through to
        # Navigator that called us
        return navigator.showMenu(menu)

    # Return the uiMgrLog object instance
    def getLog(self):
        return self.__log
        
   # Return the uiMgrResponder object instance
    def getResponder(self):
        return self.__responder

    # Return true if the user will be asked, return false if we are
    # not actually interactive (replaying canned responses)
    def getIsInteractive(self):
        return not self.getResponder().hasResponses()
     
    def __init__(self, metaModel, titleString="Missing Application Title"):

        self.__metaModel = metaModel
        self.__log = uiMgrLogger()
        self.__responder = uiMgrResponder() # scripted responses

      #  self.__dialog = dialog.Dialog(dialog=DBinclude.binDir + "/dialog")
        self.__dialog = dialog.Dialog(dialog="dialog")
        self.__dialog.add_persistent_args(["--backtitle", titleString ])
        self.__dialog.add_persistent_args(["--aspect", "15" ])  # prefer 15 width to 1 height
        
        self.__navigator = Navigator(self.__dialog)

        self.__initializeMenuStructure(self.__metaModel)

# overwriting class Navigator
class Navigator:

    NAV_OK = 0
    NAV_CANCEL = 1
    NAV_HELP = 5

    NAV_MTITLE = "title"
    NAV_MITEMS = "items"
    NAV_MACTIONS = "actions"
    NAV_MBACK = "backLink"
    NAV_MOBJ = "obj"
    NAV_MPRESENT = "presentmenu"
    NAV_MHELP = "item_help"
    

    def __init__(self, userInterface):

        self.__ui = userInterface
        self.__menus = {}
        a_string = ""
        self.__string_type = a_string.__class__

    # Print out a innocuous string value so that when a Navigator is
    # printed in a log we don't get the default string with a pointer
    # (that we would have to normalize in regression tests)
    #
    # vertica.ui.Navigator.Navigator instance at 0x8aa93ec
    def __repr__(self):
        return "vertica.ui.Navigator.Navigator Instance"

    def defaultExitHandler(self, d, code):

        if code in (d.DIALOG_CANCEL, d.DIALOG_ESC):
            return True
        else:
            return False
        
    def getListChoice(self, title, items, filename):

        w = len(title)
        for choice in items:
            len_a = len(choice[0])
            len_b = len(choice[1])
            if len_a + len_b > w:
                w = len_a + len_b
        
        if w > 120:
            w = 120       
                
        w += 20
        while 1:
            (code, tag) = self.__ui.radiolist(
                title,
                width=w,
                choices=items, help_button=0)
            #if code == 5 that means the help is pressed.
            if code == 5:
                self.getHelpTextBox(filename)
            elif self.defaultExitHandler(self.__ui, code):
                return self.NAV_CANCEL, ""
            elif code == 0:
                return self.NAV_OK, tag
            
    def getMultipleChoices(self, title, items, filename):
        w = len(title)
        for choice in items:
            len_a = len(choice[0])
            len_b = len(choice[1])
            if len_a + len_b > w:
                w = len_a + len_b

        if w > 120:
            w = 120       
            
        w += 20
        while 1:
            (code, tag) = self.__ui.checklist(
                title,
                width=w,
                choices=items , help_button=0)
            #if code == 5 that means the help is pressed.
            if code == 5:
                self.getHelpTextBox(filename)
            elif self.defaultExitHandler(self.__ui, code):
                return self.NAV_CANCEL, ""
            elif code == 0:
                return self.NAV_OK, tag            

    def add_menu(self, menuName, menuTitle, menuItems, menuActions, backLink, menuPresent, item_help):
        
        if("." in menuItems[0][0:5]):
            zipped = zip(menuItems, menuActions, item_help)
            zipped.sort(key = lambda t: int(t[0].split('.')[0]) if '.' in t[0] else t[0])
            menuItems, menuActions, item_help = zip(*zipped)
            
        self.__menus[menuName] = {}
        self.__menus[ menuName ][ self.NAV_MTITLE ] = menuTitle
        self.__menus[ menuName ][ self.NAV_MITEMS ] = menuItems
        self.__menus[ menuName ][ self.NAV_MACTIONS ] = menuActions
        self.__menus[ menuName ][ self.NAV_MBACK ] = backLink
        self.__menus[ menuName ][ self.NAV_MPRESENT ] = menuPresent
        self.__menus[ menuName ][ self.NAV_MHELP ] = item_help
        self.__menus[ menuName ][ self.NAV_MOBJ ] = Menu(self.__menus[ menuName ][ self.NAV_MTITLE ], userInterface=self.__ui)
        (self.__menus[ menuName ][ self.NAV_MOBJ ]).makeChoices(self.__menus[ menuName ][ self.NAV_MITEMS ], backLink=self.__menus[ menuName ][ self.NAV_MBACK ])

    def promptForInfo(self, question, help, defaultAnswer=""):
        while 1:
            (code, answer) = self.__ui.inputbox(question, init=defaultAnswer, help_button=1,width=80, height=10)
            #if code == 5 that means the help is pressed.
            if code == 5:
                self.getHelpTextBox(help)
            elif self.defaultExitHandler(self.__ui, code):
                return self.NAV_CANCEL, ""
            elif code == 0:
                return self.NAV_OK, answer

    def promptForPassword(self, question, filename):
        while 1:
            (code, answer) = self.__ui.passwordbox(question, feedback=1, help_button=0)
            #if code == 5 that means the help is pressed.
            if code == 5:
                self.getHelpTextBox(filename)
            elif self.defaultExitHandler(self.__ui, code):
                return self.NAV_CANCEL, ""
            elif code == 0:
                return self.NAV_OK, answer            

    def fileSelect(self, startDir, message, filename, mustBeFile=True):
        while 1:
            if startDir == None or startDir == "":
                root_dir = os.sep               # This is OK for UNIX systems
                dir = os.getenv("HOME", root_dir)
                # Make sure the directory we chose ends with os.sep() so that dialog
                # shows its contents right away
                if dir and dir[-1] != os.sep:
                    dir = dir + os.sep          
            else:
                dir = startDir
            (code, path) = self.__ui.fselect(dir, 10, 50, title=message, help_button=0)
            #if code == 5 that means the help is pressed.
            if code == 5:
                self.getHelpTextBox(filename)
            elif self.defaultExitHandler(self.__ui, code):
                return self.NAV_CANCEL, ""
            elif code == 0:
                return self.NAV_OK, path
    
    #function is to open the file passed to it in the Text box for help option, or display the text passed to it
    def getHelpTextBox(self, tag_filename):     
        
        if (tag_filename and tag_filename != ""):
        
            if(tag_filename.endswith(".txt")):
        
                tag_filename = tag_filename.replace(" ", "")
                fileName = DBinclude.HELP_DIR + "/" + tag_filename
                if os.path.isfile(fileName):
                    code = self.__ui.textbox(fileName, height=0, width=0, title="HELP")
                    return
            else:
                self.__ui.msgbox(tag_filename, height=0, width=0)
                return
        
        self.__ui.msgbox("Help file %s not available" % tag_filename, height=0, width=0)
              
    #Display the specified menu, return code,tag
    def showMenu(self, nextMenu):
        return (self.__menus[ nextMenu ][ self.NAV_MOBJ ]).displayMenu()
        
    def navigate(self, uimanager, startingPoint):
        
        nextMenu = startingPoint
        while 1:
            if nextMenu == None:
                return

            # ask the UI to show the menu for us (returns a scripted
            # response or will call Navigator.showMenu above)
            code, tag = uimanager.showMenu(self, nextMenu)
            #code, tag  = (self.__menus[ nextMenu ][ self.NAV_MOBJ ]).displayMenu()
            global VBUDDYLITE_TAG
            VBUDDYLITE_TAG = tag;
            global VBUDDYLITE_MENU
            VBUDDYLITE_MENU = nextMenu
            
            if code == "help":
                    if tag == "E":
                        self.getHelpTextBox("\nExit\n****\n\nExit from vBuddy Lite.")    
                    elif (tag == "B"):
                        self.getHelpTextBox("\nBack\n****\n\nReturns to the previous menu.")
                    elif (tag == "M"):
                        helpchoice = self.__menus[ nextMenu ][ self.NAV_MHELP ]
                        helpchoicelen = len (helpchoice)
                        filename = helpchoice [ (helpchoicelen - 1) ]
                        self.getHelpTextBox(filename)
                    else:
                        fileChoice = (self.__menus[ nextMenu ][ self.NAV_MOBJ ]).getFileChoice(self.__menus[ nextMenu ][ self.NAV_MHELP ], tag)
                        self.getHelpTextBox(fileChoice)
                        nextMenu = self.__menus[ nextMenu ][ self.NAV_MPRESENT ]
            elif tag == "E":        
                sys.exit(1)
            elif tag == "M":
                nextMenu = "main"
            elif tag == "B":
                nextMenu = self.__menus[ nextMenu ][ self.NAV_MBACK ]
            elif code == 0:
                offset = int(tag) - 1
                typeOf = ((self.__menus[ nextMenu ][ self.NAV_MACTIONS ])[ offset ]).__class__
                if typeOf == self.__string_type:
                    nextMenu = (self.__menus[ nextMenu ][ self.NAV_MACTIONS ])[ offset ]
                else:
                    ((self.__menus[ nextMenu ][ self.NAV_MACTIONS ])[ offset ])()

# overwriting class Menu
class Menu:

    M_STYLE_ENUMERATED = 1
    M_STYLE_FULLY_QUALIFIED = 2

    def __defaultExitHandler(self, d, code):
        if code in (d.DIALOG_CANCEL, d.DIALOG_ESC):
            return True
        else:
            return False
        
    def __init__(self, menuTitle, menuChoices=[], menuWidth=80, menuExitHandler=None, userInterface=None):

        self.__title = menuTitle
        self.__choices = menuChoices
        self.__width = menuWidth
        self.__ui = userInterface
        if menuExitHandler != None:
            self.__exitHandler = menuExitHandler
        else:
            self.__exitHandler = self.__defaultExitHandler

    def setChoices(self, menuChoices):
        self.__choices = menuChoices

    def setExitHandler(self, menuExitHandler):
        self.__exitHandler = menuExitHandler

    def setWidth(self, menuWidth):
        self.__width = menuWidth

    def setTitle(self, menuTitle):
        self.__title = menuTitle

    def setUI(self, uiObj):
         self.__ui = uiObj

    def makeChoices(self, menuChoices, backLink=None):
        result = []
        count = 1
        for choice in menuChoices:
        
            if("." in choice):
                key = choice.split(".")[0]
                res = (key, choice.split(".")[1])
            else:
                key = str(count)
                res = (key, choice)
            result.append(res)
            count += 1
        if backLink != None:
            res = ("B", "Back")
            result.append(res) 
        else:
            res = ("E", "Exit")
            result.append(res)            
        self.setChoices(result)
    
    def getFileChoice(self, menuChoiceshelp, tag):
        tag = int(tag)
        choice = menuChoiceshelp[(tag - 1)]
        return choice
        
    def displayMenu(self, userInterface=None):
        if userInterface != None:
            ui = userInterface
        else:
            ui = self.__ui
        if ui != None:
            while 1:
                (code, tag) = ui.menu(self.__title,
                                      width=self.__width,
                                      menu_height=len(self.__choices), # no scrolling
                                      height=len(self.__choices)+8,
                                      choices=self.__choices , help_button=0)
                if not self.__exitHandler(ui, code):
                    break
                else:
                    return code, "B"
            return code, tag
        else:
            print "Primitive menus not yet implemented"
            sys.exit(-1)


##
## Begin monolithic main routine. This is pretty much copied from admintools
##

# Globals
exit_code = 0
toolName = None
toolArgs = []
options = None
makeUniquePorts = False
showNodes = False
nonInteractive = False
userName = pwd.getpwuid(os.getuid())[0]

if userName == "root":
    print "\nRoot user is not allowed to use this tool."
    print "Try again as the DB administrative user."
    sys.exit(1)

#
# confirm the installer has been run
#
if not os.path.exists(DBinclude.ADMINTOOLS_CONF):
    print """
ERROR: %s does not exist.
This file is created by the installer upon successful completion.
You must successfully run %s/install_vertica before running vBuddyLite.
""" % (DBinclude.ADMINTOOLS_CONF,DBinclude.sbinDir)
    sys.exit(1)


CONFIG_INFO_DIR = DBinclude.CONFIG_INFO_DIR

#
# check shell.  Why does it matter what the login shell is? What's the CURRENT
# shell?
#
loginShell = pwd.getpwuid(os.getuid())[6]
if loginShell != "/bin/bash":
    print """
WARNING:  vBuddyLite depend on your login shell being /bin/bash.
Your login shell is currently set to %s.  If you continue to run vBuddyLite
with this default shell you may experience technical difficulties
""" % loginShell
    if not DBfunctions.yesno("Do you wish to continue? "):
        sys.exit()

for neededPath in [ DBinclude.CONFIG_SHARE_DIR, DBinclude.CONFIG_USER_DIR ]:
   if not (os.path.exists(neededPath)):
      cmd = "mkdir -p " + neededPath
      os.system(cmd)
      cmd = "chmod 777 "+ neededPath
      os.system(cmd)

userPath = DBinclude.CONFIG_USER_DIR + "/" + userName
if not (os.path.exists(userPath)):   
   cmd = "mkdir -p " + userPath
   os.system(cmd)
   cmd = "chmod go-w "+ userPath
   os.system(cmd)
   if os.path.exists( CONFIG_INFO_DIR ):
      #
      # if the old configInfo directory exists, and the files there are owned
      # by this user, then move them to the right locations.
      #
      if os.path.exists(CONFIG_INFO_DIR):
         for fName in ["/siteinfo.dat","/dbinfo.dat"]:
            if os.path.exists(CONFIG_INFO_DIR+fName):
               stblock = os.stat(CONFIG_INFO_DIR+fName)
               myUID = os.getuid()
               if myUID == stblock.st_uid:
                  os.rename( CONFIG_INFO_DIR+fName, userPath+fName)

try:
  try: # again
     lastChanceFileName = vBuddyLiteErrorLog
     lastChanceFile = None
     ac = adminController(makeUniquePorts)
     if not ac.isOkToRun():
        print "User %s is not allowed to run vBuddyLite. To enable access run install_vertica" % userName
        sys.exit(1)

     lastChanceFile = open( lastChanceFileName, "a")
     ac.runApp()
     
     lastChanceFile.flush()
     lastChanceFile.close()

  except KeyboardInterrupt:
     os.system("reset")
     print "vBuddyLite exited on ctrl-c or other user interrupt"
  except SystemExit:
     # raised by sys.exit() so we let it go for now
     pass
  except dialog.DialogError:
     print "vBuddyLite failed to create an interface object." 
     print "Please resize your window to a larger width and height"
     exit_code = 1
  except FileLocker.FileLockException:
      print "vBuddyLite was unable read %s/config/adminTools.conf" % DBinclude.DB_DIR
      print "due to a conflict with another process that was writing to this"
      print "file.  Please retry or if necessary remove the .lock file"
      print "in the config directory"
  except OSError, e:
      print "A system-related error has occurred.  For more information"
      print "see the error number returned below. Exiting."
      print "%s" % e 
  except socket.gaierror, gai:
      print "An error in the python socket library occurred.  This is"
      print "probably due to a network lookup error with one of the hosts"
      print "in your cluster."
      print "%s" % gai
  except:
     # last chance handler - we land here for any unhandled exceptions
     # raised in the system.  We log the traceback to a file in /tmp
     # (or stdout if we couldn't write to /tmp), print some useful erro
     # text, and exit.
     errmsg = sys.exc_info()[1]
     raisedError = sys.exc_info()[0]
     if lastChanceFile == None:
       lastChanceFile = sys.stdout
     lastChanceFile.write( "-" * 32 )
     lastChanceFile.write("\n")
     htime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())         
     lastChanceFile.write("handler invoked: %s\n" % htime )
     rerr = str(raisedError)
     lastChanceFile.write("raised error: %s\n" % rerr )
     traceback.print_exc( file=lastChanceFile )

     print "\n\n\nvBuddyLite Last Chance Error Handler running..."
     print "raised error: ",raisedError
     print "error message: %s" % errmsg
     print "trace file: ",lastChanceFileName
     print "REPORT THIS INFORMATION TO TECHNICAL SUPPORT"
     print "AND INCLUDE CONTENTS OF THE TRACE FILE IN YOUR REPORT"
     exit_code = 1

finally:
   # close ssh connections pool
   # disable ^C during this time as we want to clean up all our connections
   DisableKeyboardInterrupt()
   pool = adapterpool.AdapterConnectionPool_3.Instance()
   pool.close()
   EnableKeyboardInterrupt()
   
   sys.exit(exit_code)
