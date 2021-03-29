#!/bin/sh

set -x -e

day=$1
p3months=`date -d "$day -90 day" +%Y%m%d`
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

# input
device_distance_day_pre=${dm_mobdi_tmp}.device_distance_day_pre
# output
device_distance_risk_3month=${dm_mobdi_tmp}.device_distance_risk_3month

hive -e"
SET mapreduce.map.memory.mb=6144;
SET mapreduce.map.java.opts='-Xmx4g';
SET mapreduce.child.map.java.opts='-Xmx4g';
set mapreduce.reduce.memory.mb=6144;
SET mapreduce.reduce.java.opts='-Xmx4g';
SET mapreduce.map.java.opts='-Xmx4g';
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

with distance_day90 as(
  select device, avg(distance_byday) as mean_distance
  from 
  (
    select device, day, sum(distance) as distance_byday
    from $device_distance_day_pre
    where day between '$p3months' and '$day'
    group by device, day
  ) as a 
  group by device
),
distance_day_night90 as(
  select device, avg(distance_byday) as mean_distance 
  from 
  (
    select device, day, sum(distance) as distance_byday
    from $device_distance_day_pre
    where day between '$p3months' and '$day' and (hour(start_time) >= 22 and hour(start_time) <= 23) or (hour(start_time) >= 0 and hour(start_time) < 6)
    group by device, day
  ) as a 
  group by device
),
distance_all90 as(
  select device, sum(distance) as distance_all
  from $device_distance_day_pre
  where day between '$p3months' and '$day'
  group by device
)
insert overwrite table $device_distance_risk_3month
select device, avg(risk) as distance_risk_3month
from 
(
  select device, distance, 
  case 
    when distance = 0 then 1 
    when distance > 0 and distance <= 100 then 0.8
    when distance > 100 and distance <= (11734+1.5*(11734-113)) then 0
    when distance > (11734+1.5*(11734-113)) and distance <= 400000 then distance*1/(400000-(11734+1.5*(11734-113))) + (1 - 400000*1/(400000-(11734+1.5*(11734-113))))
    when distance > 400000 then 1
  end as risk
  from 
  (
    select device, mean_distance as distance
    from distance_day90
  ) as a 
  union all 
  select device, distance, 
  case 
    when distance <= (3573+1.5*(3573-15)) then 0
    when distance > (3573+1.5*(3573-15)) and distance <= 200000 then distance*1/(200000-(3573+1.5*(3573-15))) + (1 - 200000*1/(200000-(3573+1.5*(3573-15))))
    when distance > 200000 then 1
  end as risk
  from 
  (
    select device, mean_distance as distance
    from distance_day_night90
  ) as b 
  union all 
  select device, distance, 
  case 
    when distance = 0 then 1 
    when distance > 0 and distance <= 100 then 0.8
    when distance > 100 and distance <= (164928+1.5*(164928-378)) then 0
    when distance > (164928+1.5*(164928-378)) and distance <= 30000000 then distance*1/(30000000-(164928+1.5*(164928-378))) + (1 - 30000000*1/(30000000-(164928+1.5*(164928-378))))
    when distance > 30000000 then 1
  end as risk
  from 
  (
    select device, distance_all as distance
    from distance_all90
  ) as c 
) as d 
group by device
"