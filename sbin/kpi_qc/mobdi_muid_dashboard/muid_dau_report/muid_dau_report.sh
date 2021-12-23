#!/bin/bash

set -e -x
if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi
day=$1
HADOOP_USER_NAME=dba hive -e"
set hive.exec.parallel=true;
SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx6144M' -XX:+UseG1GC;;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144M';
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;

with ids as (  
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('oiid_in_muid','ieid_in_muid','isid_in_muid','pid_in_muid_android') 
group by device 
),
applist as ( 
select device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type='applist_in_muid' 
group by device
),
lbs as (
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di   
where day='${day}' and plat=1 and
muid_type ='gps_base_wifi_in_muid_android' 
group by device
) 


insert overwrite table  mobdi_muid_dashboard.muid_dau_report partition(day=$day)
select any_one,id_app_lbs,id_app,id_lbs from 
(---有id/applist/lbs
 select count(1) any_one 
 from (
     select device from 
     (select device from ids 
     union all select device 
     from applist union all select device from lbs 
     )tmp1 group by device )xxx 
   )any
join
(-----有id&applist&lbs
select count(ids.device) id_app_lbs
    from ids 
    inner join applist 
    inner join lbs 
    on ids.device=applist.device and ids.device=lbs.device )a
join 
(-----有id&applist
select count(ids.device) id_app
    from ids 
    inner join applist 
    on  ids.device=applist.device 
    )b
join
(-----有id&lbs
select count(ids.device) id_lbs
    from ids 
    inner join lbs 
    on  ids.device=lbs.device )c


"
