#!/bin/sh

set -x -e

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp

# input
device_distance_day_pre=$tmpdb.device_distance_day_pre
# output
device_distance_risk_1month=$tmpdb.device_distance_risk_1month

day=$1
p1months=`date -d "$day -30 day" +%Y%m%d`

hive -e"
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
SET mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';
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


with distance_day30 as(
  select device, avg(distance_byday) as mean_distance
  from 
  (
    select device, day, sum(distance) as distance_byday
    from $device_distance_day_pre
    where day between '$p1months' and '$day'
    group by device, day
  ) as a 
  group by device
),
distance_day_night30 as(
  select device, avg(distance_byday) as mean_distance 
  from 
  (
    select device, day, sum(distance) as distance_byday
    from $device_distance_day_pre
    where day between '$p1months' and '$day' and (hour(start_time) >= 22 and hour(start_time) <= 23) or (hour(start_time) >= 0 and hour(start_time) < 6)
    group by device, day
  ) as a 
  group by device
),
distance_all30 as(
  select device, sum(distance) as distance_all
  from $device_distance_day_pre
  where day between '$p1months' and '$day'
  group by device
)
insert overwrite table $device_distance_risk_1month
select device, avg(risk) as distance_risk_1month
from 
(
  select device, distance, 
  case 
    when distance = 0 then 1 
    when distance > 0 and distance <= 100 then 0.8
    when distance > 100 and distance <= (10441+1.5*(10441-109)) then 0
    when distance > (10441+1.5*(10441-109)) and distance <= 400000 then distance*1/(400000-(10441+1.5*(10441-109))) + (1 - 400000*1/(400000-(10441+1.5*(10441-109))))
    when distance > 400000 then 1
  end as risk
  from 
  (
    select device, mean_distance as distance
    from distance_day30
  ) as a 
  union all 
  select device, distance, 
  case 
    when distance <= (3084+1.5*(3084-15)) then 0
    when distance > (3084+1.5*(3084-15)) and distance <= 200000 then distance*1/(200000-(3084+1.5*(3084-15))) + (1 - 200000*1/(200000-(3084+1.5*(3084-15))))
    when distance > 200000 then 1
  end as risk
  from 
  (
    select device, mean_distance as distance
    from distance_day_night30
  ) as b 
  union all 
  select device, distance, 
  case 
    when distance = 0 then 1 
    when distance > 0 and distance <= 100 then 0.8
    when distance > 100 and distance <= (84627+1.5*(84627-309)) then 0
    when distance > (84627+1.5*(84627-309)) and distance <= 10000000 then distance*1/(10000000-(84627+1.5*(84627-309))) + (1 - 10000000*1/(10000000-(84627+1.5*(84627-309))))
    when distance > 10000000 then 1
  end as risk
  from 
  (
    select device, distance_all as distance
    from distance_all30
  ) as c 
) as d 
group by device
"
