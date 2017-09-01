create resource pool pool_5_1_2 
  priority 50
  maxmemorysize '80%'
  plannedconcurrency 2
  maxconcurrency 3
  runtimepriority HIGH
  ;
  
grant usage on resource pool pool_5_1_2  to dbadmin;

