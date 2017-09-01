
-- PARAMETER MaxClientSessions: for concurrent queries and data loading jobs. default: 50
select set_config_parameter('MaxClientSessions', 650);

-- -- PARAMETER CompressNetworkData: for network bound. default: 0
-- select set_config_parameter('CompressNetworkData', 1);

-- -- PARAMETER MultiLevelNetworkRoutingFactor: Turn on multi-level data network routing if it would reduce stream count by this factor, default: 1000
-- select set_config_parameter('MultiLevelNetworkRoutingFactor', -1);
-- 
-- -- PARAMETER EnableCooperativeParse: If true and a chunker is defined for the corresponding parser multiple parse threads can cooperate to parse the output of a single source, default: 1
-- select set_config_parameter('EnableCooperativeParse', 1);
-- 
-- -- PARAMETER MaxQueryRetries: avoid The number of times the system might try to re-run a query if the first run does not succeed.
-- -- select set_config_parameter('MaxQueryRetries', 3); -- default: 3

-- ---- avoid "too many ROS container..."
-- select set_config_parameter('MoveOutInterval', 1*3600); -- default: 300
-- select set_config_parameter('MergeOutInterval', 600); -- default: 600
-- select set_config_parameter('ContainersPerProjectionLimit', 1024000); -- default: 1024
-- 
---- PARAMETER RecoveryDirtyTxnWait: Seconds to wait for dirty transactions before cancelling the session.
--select set_config_parameter('RecoveryDirtyTxnWait', 500); -- default: 300


-- for extension: 5->3->5
select SET_SCALING_FACTOR(3); -- default: 4
select enable_local_segments();
 
-- TODO: PARAMETER EnableDataTargetParallelism: Enable the multi-thread processing of DataTarget so data load can run faster on powerful multi-core machines. default: 1
-- select set_config_parameter('EnableDataTargetParallelism', 0); 


-- RESOURCE POOL usage: 
--      set session resource_pool=load_pool; 
--    or:
--      GRANT USAGE ON RESOURCE POOL load_pool to loader;
--      alter user loader RESOURCE POOL load_pool;

-- RESOURCE POOL all: adjust for cpu cores 
\a
\t
\o /tmp/tmp.sql
select distinct 'alter resource pool ' || pool_name || ' plannedconcurrency 12; ' from resource_pool_status where planned_concurrency > 12;
\o
\a
\t
\i /tmp/tmp.sql


-- RESOURCE POOL general: preserve 1 concurrency/memory for others
alter resource pool general
   plannedconcurrency 12 
   maxconcurrency 11 
   queuetimeout NONE;

-- for ad-hoc queries
drop resource pool adhoc_query_pool;
create resource pool adhoc_query_pool 
  priority 100
  runtimepriority HIGH 
  runtimeprioritythreshold 0
  plannedconcurrency 2
  maxconcurrency 1
  queuetimeout NONE;

-- for T06/T07 loading
drop resource pool loading_pool;
create resource pool loading_pool 
   plannedconcurrency 12 
   maxconcurrency 11 
  queuetimeout NONE;


-- for 3.3.7.1.	多任务并发查询
drop resource pool concurrent_large_query_pool;
create resource pool concurrent_large_query_pool 
  priority 0 
  runtimepriority HIGH 
  runtimeprioritythreshold 0
  executionparallelism 6
  plannedconcurrency 21
  maxconcurrency 20
  queuetimeout NONE;

drop resource pool concurrent_small_query_pool;
create resource pool concurrent_small_query_pool 
  priority 100
  runtimepriority HIGH
  runtimeprioritythreshold 0
  executionparallelism 1
  plannedconcurrency 12
  maxconcurrency 9
  queuetimeout NONE;


-- for 3.3.7.2 concurrent inserts
drop resource pool concurrent_large_insert_pool;
create resource pool concurrent_large_insert_pool 
  priority 0 
  runtimepriority HIGH
  runtimeprioritythreshold 0
  executionparallelism default
  plannedconcurrency 12
  maxconcurrency 11
  queuetimeout NONE;

drop resource pool concurrent_smallinsert_pool;
create resource pool concurrent_smallinsert_pool 
  priority 0 
  runtimepriority HIGH
  runtimeprioritythreshold 0
  plannedconcurrency 12
  maxconcurrency 11
  queuetimeout NONE;


-- for 3.3.7.3 mixed workload(load+con_sel+ins+del)
drop resource pool mixed_load_pool;
create resource pool mixed_load_pool 
  priority 0 
  runtimepriority MEDIUM 
  runtimeprioritythreshold 0
  plannedconcurrency 13
  maxconcurrency 2
  queuetimeout NONE;

drop resource pool mixed_large_query_pool;
create resource pool mixed_large_query_pool 
  priority 0 
  runtimepriority MEDIUM 
  runtimeprioritythreshold 0
  executionparallelism 6
  plannedconcurrency 13
  maxconcurrency 2
  queuetimeout NONE;

drop resource pool mixed_insert_pool;
create resource pool mixed_insert_pool 
  priority 0
  runtimepriority MEDIUM 
  runtimeprioritythreshold 0
  plannedconcurrency 13
  maxconcurrency 4 
  queuetimeout NONE;

drop resource pool mixed_small_query_pool;
create resource pool mixed_small_query_pool 
  priority -10 
  runtimepriority MEDIUM 
  runtimeprioritythreshold 0 
  executionparallelism 1
  plannedconcurrency 13
  maxconcurrency 4
  queuetimeout NONE;


-- for 3.3.7.4 mixed workload2(copy+count)
drop resource pool mixed2_load_pool;
create resource pool mixed2_load_pool 
  priority 0 
  runtimepriority MEDIUM 
  runtimeprioritythreshold 0
  plannedconcurrency 12 
  maxconcurrency 11 
  queuetimeout NONE;

drop resource pool mixed2_query_pool;
create resource pool mixed2_query_pool 
  priority 100 
  runtimepriority HIGH 
  runtimeprioritythreshold 0 
  plannedconcurrency 12 
  maxconcurrency 1 
  queuetimeout NONE;



-- for 3.3.8 export
drop resource pool exporting_pool;
create resource pool exporting_pool 
  plannedconcurrency 12
  maxconcurrency 11
  queuetimeout NONE;


-- for 3.4.1 stress
drop resource pool stress_query_pool;
create resource pool stress_query_pool 
  priority 0 
  runtimepriority HIGH 
  executionparallelism 1
  runtimeprioritythreshold 0 
  plannedconcurrency 24
  maxconcurrency 22 
  queuetimeout NONE;

-- for 3.4.2 stability
drop resource pool stabilityquery_pool;
create resource pool stabilityquery_pool 
  priority 0 
  runtimepriority HIGH 
  executionparallelism 1
  runtimeprioritythreshold 0 
  plannedconcurrency 24
  maxconcurrency 23 
  queuetimeout NONE;


-- for 3.5 availability
drop resource pool availability_query_pool;
create resource pool availability_query_pool 
  priority 0 
  runtimepriority HIGH 
  runtimeprioritythreshold 0
  plannedconcurrency 3 
  maxconcurrency 2 
  queuetimeout NONE;


-- for 3.6 scalability
drop resource pool scalability_query_pool;
create resource pool scalability_query_pool 
  priority 0 
  runtimepriority HIGH 
  runtimeprioritythreshold 0
  plannedconcurrency 2 
  maxconcurrency 1 
  queuetimeout NONE;


--  -- 60000000/?=?
-- drop resource pool xxx_pool;
-- create resource pool xxx_pool 
--  priority 100 
--  runtimepriority HIGH
--  runtimeprioritythreshold 0
--  executionparallelism default
--  plannedconcurrency 12
--  maxconcurrency 11
--  queuetimeout NONE;
-- 


-- view modified parameters
\echo Modified parameters:
select parameter_name, current_value, default_value from configuration_parameters where current_value <> default_value;
/*
select parameter_name, current_value, default_value from configuration_parameters where current_value <> default_value;
   parameter_name    | current_value | default_value 
---------------------+---------------+---------------
 MaxClientSessions   | 150           | 50
 CompressNetworkData | 1             | 0
(2 rows)
*/

select * from elastic_cluster;

select * from resource_pools 
  where name not in (select name from resource_pool_defaults);
/*

*/

select c.* from resource_pools c, resource_pool_defaults d
 where c.name=d.name
   and (
     c.memorysize::varchar <> d.memorysize::varchar
     or c.maxmemorysize::varchar <> d.maxmemorysize::varchar
     or c.executionparallelism::varchar <> d.executionparallelism::varchar
     or c.priority::varchar <> d.priority::varchar
     or c.runtimepriority::varchar <> d.runtimepriority::varchar
     or c.runtimeprioritythreshold::varchar <> d.runtimeprioritythreshold::varchar
     or c.queuetimeout::varchar <> d.queuetimeout::varchar
     or c.runtimecap::varchar <> d.runtimecap::varchar
     or c.plannedconcurrency::varchar <> d.plannedconcurrency::varchar
     or c.maxconcurrency::varchar <> d.maxconcurrency::varchar
     or c.singleinitiator::varchar <> d.singleinitiator::varchar
   );
/*
      pool_id      |   name   | is_internal | memorysize | maxmemorysize | executionparallelism | priority | runtimepriority | runtimeprioritythreshold | queuetimeout | plannedconcurrency | maxconcurrency | runtimecap | singleinitiator | cpuaffinityset | cpuaffinitymode 
-------------------+----------+-------------+------------+---------------+----------------------+----------+-----------------+--------------------------+--------------+--------------------+----------------+------------+-----------------+----------------+-----------------

*/

select d.* from resource_pools c, resource_pool_defaults d
 where c.name=d.name
   and (
     c.memorysize::varchar <> d.memorysize::varchar
     or c.maxmemorysize::varchar <> d.maxmemorysize::varchar
     or c.executionparallelism::varchar <> d.executionparallelism::varchar
     or c.priority::varchar <> d.priority::varchar
     or c.runtimepriority::varchar <> d.runtimepriority::varchar
     or c.runtimeprioritythreshold::varchar <> d.runtimeprioritythreshold::varchar
     or c.queuetimeout::varchar <> d.queuetimeout::varchar
     or c.runtimecap::varchar <> d.runtimecap::varchar
     or c.plannedconcurrency::varchar <> d.plannedconcurrency::varchar
     or c.maxconcurrency::varchar <> d.maxconcurrency::varchar
     or c.singleinitiator::varchar <> d.singleinitiator::varchar
   );
/*
      pool_id      |   name   | memorysize | maxmemorysize | executionparallelism | priority | runtimepriority | runtimeprioritythreshold | queuetimeout | runtimecap | plannedconcurrency | maxconcurrency | singleinitiator | cpuaffinityset | cpuaffinitymode 
-------------------+----------+------------+---------------+----------------------+----------+-----------------+--------------------------+--------------+------------+--------------------+----------------+-----------------+----------------+-----------------

*/
