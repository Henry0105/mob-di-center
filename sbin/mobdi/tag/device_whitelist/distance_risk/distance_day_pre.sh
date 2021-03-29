#!/bin/sh

set -x -e

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

# input
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di
# output
device_distance_day_pre=${dm_mobdi_tmp}.device_distance_day_pre

day=$1
# 每个月16号执行
pday=`date -d "$day -30 days" +%Y%m`16

hive -e"
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

with distance_pre as 
(
  select device, lat, lon, start_time, end_time, day, network, type, data_source, orig_note1, orig_note2, get_geohash(lat, lon, 5) as geohash5
  from $dws_device_location_staying_di
  where day >= '$pday' and day <= '$day' and type in ('gps', 'wifi') and data_source not in ('unknown_gps', 'unknown_auto')
),
strange_wifi as(
  select orig_note1, sum(cnt) as num
  from 
  (
    select orig_note1, geohash5, count(*) as cnt
    from  distance_pre
    where  type = 'wifi' and orig_note1 like 'bssid=%'
    group by orig_note1, geohash5
  ) as m 
  group by orig_note1
  having count(*) > 1
),
union_distance_info as(
  select *
  from 
  (
    select a.*
    from 
    (
      select *
      from distance_pre
      where type = 'wifi'
    ) as a 
    left join strange_wifi
    on a.orig_note1 = strange_wifi.orig_note1
    where strange_wifi.orig_note1 is null
    union all 
    select * 
    from distance_pre
    where type <> 'wifi'
  ) as m 
  distribute by device sort by device, day, start_time
)
insert overwrite  table $device_distance_day_pre partition(day)
select device, lat, lon, lat1, lon1, start_time, end_time, get_distance(lat, lon, lat1, lon1) as distance, network, type, data_source, orig_note1, orig_note2, geohash5,day
from 
(
  select device, lat, lon, lead(lat, 1) over(partition by device order by day, start_time) as lat1, lead(lon, 1) over(partition by device order by day, start_time) as lon1, start_time, end_time, day, network, type, data_source, orig_note1, orig_note2, geohash5
  from union_distance_info
) as a 
where lat1 is not null and lon1 is not null
"




 








