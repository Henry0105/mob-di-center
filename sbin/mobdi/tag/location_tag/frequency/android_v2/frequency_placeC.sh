#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01

p3month=`date -d "$days -3 month" +%Y%m`

p1month=`date -d "$days -1 month" +%Y%m`

#input
tmp_device_location_summary_monthly=dm_mobdi_tmp.tmp_device_location_summary_monthly
tmp_device_frequency_place=dm_mobdi_tmp.tmp_device_frequency_place

prepare_sql="
select device,lon,lat,time,unix_timestamp(concat(day, ' ', time), 'yyyyMMdd HH:mm:ss') as datetime,day
from
(
   select device,lon,lat,time,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 4 and station = 'wifi' and wifi_type<>''
   union all
   select device,lon,lat,time,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 2 and station <> '4g'
   union all
   select device,lon,lat,time,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 3 and wifi_type <> '1'
)a
left semi join
(select device from $tmp_device_frequency_place where stage in ('A','B')  group by device having count(1) < 10) b
on a.device = b.device
"
spark2-submit --master yarn \
		      --executor-memory 9G \
			  --driver-memory 15G \
			  --executor-cores 3 \
        --name "frequency C" \
        --deploy-mode cluster \
			  --class com.youzu.mob.location.frequency.Frequency \
			  --conf spark.dynamicAllocation.enabled=true \
			  --conf spark.dynamicAllocation.minExecutors=10 \
        --conf spark.network.timeout=1200s \
        --conf spark.executor.heartbeatInterval=30s \
        --conf spark.yarn.executor.memoryOverhead=4096 \
			  --conf spark.dynamicAllocation.maxExecutors=100 \
			  --conf spark.default.parallelism=10000 \
			  --conf spark.sql.shuffle.partitions=10000 \
			  --conf spark.driver.maxResultSize=5g \
			  /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar C "$prepare_sql" 1
