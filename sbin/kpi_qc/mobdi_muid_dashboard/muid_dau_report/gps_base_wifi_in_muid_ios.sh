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
insert overwrite table mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di partition(day=$day,muid_type='gps_base_wifi_in_muid_ios')
select a.device, '2' as plat
from 
(
select device
from 
(
  select deviceid as device
  from dm_mobdi_master.dwd_log_run_new_di
  where day = '$day'  and plat = 2
  group by deviceid
  union all
  select deviceid as device
  from dm_mobdi_master.dwd_app_runtimes_stats_sec_di
  where day = '$day'  and plat = 2
  group by deviceid
    union all
  select deviceid as device
  from dm_mobdi_master.dwd_pv_sec_di
  where day = '$day'  and plat = 2
  group by deviceid
    union all
  select deviceid as device
  from dm_mobdi_master.dwd_mdata_nginx_pv_di
  where day = '$day'  and plat = 2
  group by deviceid
)m
group by device
) as a 
left semi join 
(
  select device
  from 
  (
    select deviceid as device
    from dm_mobdi_master.dwd_location_info_sec_di
    where day=$day and plat = '2' and latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 and abs(latitude)<=90 and abs(longitude)<=180
    group by deviceid
    union all
    select deviceid as device
    from dm_mobdi_master.dwd_auto_location_info_sec_di
    where day=$day and plat = '2' and latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 and abs(latitude)<=90 and abs(longitude)<=180
    group by deviceid
    union all 
    select deviceid as device
    from dm_mobdi_master.dwd_t_location_sec_di
    where day=$day and plat = '2' and latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 and abs(latitude)<=90 and abs(longitude)<=180
    group by deviceid
    union all
    select  device
    from dm_mobdi_master.dwd_log_wifi_info_sec_di
    where day=$day and plat = '2' 
    and trim(bssid) not in ( '00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff', '', 'null', 'NULL') and bssid is not null and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
    group by device
  ) as a 
  group by device
) as b 
on a.device = b.device;
"
