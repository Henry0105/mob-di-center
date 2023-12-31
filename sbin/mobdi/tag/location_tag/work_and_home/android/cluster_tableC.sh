#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01
source /home/dba/mobdi_center/conf/hive-env.sh
p3month=`date -d "$days -3 month" +%Y%m`

p1month=`date -d "$days -1 month" +%Y%m`
tmpdb=$dm_mobdi_tmp
#input
tmp_device_location_stage_pre=$tmpdb.tmp_device_location_stage_pre
tmp_device_location_summary_monthly=$tmpdb.tmp_device_location_summary_monthly
tmp_device_live_place=$tmpdb.tmp_device_live_place
tmp_device_work_place=$tmpdb.tmp_device_work_place

#out
tmp_device_location_stage_pre=$tmpdb.tmp_device_location_stage_pre

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

insert overwrite table $tmp_device_location_stage_pre partition(stage='C')
select a.device,lon,lat,time,datetime,day
from
(
   select device,lon,lat,time,datetime,day
   from  $tmp_device_location_stage_pre
   where stage ='A'
   union all
   select device,lon,lat,time,unix_timestamp(concat(day, ' ', time), 'yyyyMMdd HH:mm:ss') as datetime,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 2
   union all
   select device,lon,lat,time,unix_timestamp(concat(day, ' ', time), 'yyyyMMdd HH:mm:ss') as datetime,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 3
   union all
   select device,lon,lat,time,unix_timestamp(concat(day, ' ', time), 'yyyyMMdd HH:mm:ss') as datetime,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 4 and station ='wifi' and wifi_type !=''
)a
left  join
(
   select liveplace.device
   from
   (
      select device from $tmp_device_live_place  where stage in ('A','B') and confidence >= 0.5 group by device
   ) liveplace
  inner join
   (
      select device from $tmp_device_work_place  where stage in ('A','B') and confidence >= 0.5 group by device
   ) workplace
   on liveplace.device = workplace.device
)t
on a.device = t.device
where t.device is null
"

prepare_sql="
select * from $tmp_device_location_stage_pre where stage ='C'
"

spark2-submit --master yarn \
		      --executor-memory 10G \
			  --driver-memory 10G \
        --executor-cores 3 \
        --name "dbscan 3monthly table C" \
			  --deploy-mode cluster \
        --class com.youzu.mob.location.workandlive.WorkAndLivePlaceClusterWithNothing \
			  --conf spark.dynamicAllocation.enabled=true \
			  --conf spark.dynamicAllocation.minExecutors=10 \
        --conf spark.network.timeout=1200s \
        --conf spark.executor.heartbeatInterval=30s \
			  --conf spark.dynamicAllocation.maxExecutors=160 \
			  --conf spark.default.parallelism=10000 \
			  --conf spark.sql.shuffle.partitions=10000 \
			  --conf spark.executor.memoryOverhead=4096 \
			  --conf spark.driver.maxResultSize=5g \
        --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+UseG1GC "     \
        /home/dba/mobdi_center/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "C" "${prepare_sql}" 1
