# !/bin/bash

day=$1


p3month=`date -d "$day -3 month" +%Y%m%d`
pmonth=`date -d "$day -1 month" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

### 源表
tmp_device_location_stage_pre=dm_mobdi_tmp.tmp_device_location_stage_pre

### 目标表
#dim_latlon_blacklist_mf=dm_mobdi_mapping.dim_latlon_blacklist_mf




hive -v -e"

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;

SET mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

------  更新数据 latlon 三个月均为出现黑名单表中 则删除该条记录
insert overwrite table $dim_latlon_blacklist_mf partition(day='$day')
select lat,lon,cnt,update_day,stage from (
select lat,lon,max(cnt) as cnt,max(update_day) as update_day,stage from (
   select lat,lon,cnt,'$day' as update_day,stage from
   (
   select lat,lon,count(distinct(device)) as cnt, 'A' AS stage
   from $tmp_device_location_stage_pre
   where stage='A'
   group by lat,lon
   ) t1
   where cnt>300
   union all
   select lat,lon,cnt,'$day' as update_day,stage from
   (
   select lat,lon,count(distinct(device)) as cnt, 'B' AS stage
   from $tmp_device_location_stage_pre
   where stage='B'
   group by lat,lon
   ) t1
   where cnt>3000

union all
select lat,lon,cnt,'$day' as update_day,stage from
(
select lat,lon,count(distinct(device)) as cnt, 'C' AS stage
from $tmp_device_location_stage_pre
where stage='C'
group by lat,lon
) t1
where cnt>3000

union all
select lat,lon,cnt,'$day' as update_day,stage from
(
select lat,lon,count(distinct(device)) as cnt, 'D' AS stage
from $tmp_device_location_stage_pre
where stage='D'
group by lat,lon
) t1
where cnt>3000

union all
   select
   lat,lon,cnt,update_day,stage
   from
   $dim_latlon_blacklist_mf
   where day=GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_latlon_blacklist_mf', 'day')
)tt1
group by stage,lat,lon
)tt2
where update_day>'${p3month}';
"