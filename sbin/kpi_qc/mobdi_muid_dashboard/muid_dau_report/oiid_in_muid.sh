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
set hive.support.quoted.identifiers=None;
insert overwrite table mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di partition(day=$day,muid_type='oiid_in_muid')
select a.device, '1' as plat
from 
(
select muid as device
from 
(
  select muid 
  from dm_mobdi_master.dwd_log_run_new_di
  where day = '$day'  and plat = 1
  group by muid
  union all
  select muid 
  from dm_mobdi_master.dwd_app_runtimes_stats_sec_di
  where day = '$day'  and plat = 1
  group by muid
    union all
  select muid 
  from dm_mobdi_master.dwd_pv_sec_di
  where day = '$day'  and plat = 1
  group by muid
    union all
  select muid 
  from dm_mobdi_master.dwd_mdata_nginx_pv_di
  where day = '$day'  and plat = 1
  group by muid
)m
group by muid
) as a 
left semi join 
(
select device
from dm_mobdi_topic.dws_device_oiid_di
where day =$day
group by device
) as b 
on a.device = b.device;
"
