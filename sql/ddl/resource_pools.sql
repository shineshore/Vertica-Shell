-- for 4.1.1 loading
drop resource pool loading_pool;
create resource pool loading_pool 
   plannedconcurrency 12 
   maxconcurrency 11 
  queuetimeout NONE;

-- for 4.1.2 export
drop resource pool exporting_pool;
create resource pool exporting_pool 
  plannedconcurrency 12
  maxconcurrency 11
  queuetimeout NONE;
  


create resource pool concurrent_query_pool 
  priority 10 
  runtimepriority HIGH 
  runtimeprioritythreshold 0
  executionparallelism 6
  plannedconcurrency 14
  maxconcurrency 13
  queuetimeout NONE;
  
  
  
create resource pool con_10_query_pool 
  priority 10 
  runtimepriority HIGH 
  runtimeprioritythreshold 0
  executionparallelism 6
  plannedconcurrency 14
  maxconcurrency 13
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
  
  
-- for 5.1.1 workload management  
create resource pool workload_mgt_pool 
  priority 100 
  CPUAFFINITYMODE EXCLUSIVE
  CPUAFFINITYSET '6%'
  MEMORYSIZE '5%'  
  queuetimeout NONE;
  
-- for 5.1.2 workload management    
  create resource pool concurrent_mgt_pool 
  priority 100 
  runtimepriority HIGH
  runtimeprioritythreshold 0
  plannedconcurrency 4
  maxconcurrency 3
  queuetimeout NONE;
  
  
  create resource pool mixed_query_pool 
  priority 10 
  runtimepriority MEDIUM 
  runtimeprioritythreshold 0
  executionparallelism 6
  plannedconcurrency 13
  maxconcurrency 6
  queuetimeout NONE;
