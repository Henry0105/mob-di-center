#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01

p3month=`date -d "$days -3 month" +%Y%m`

p1month=`date -d "$days -1 month" +%Y%m`
tmpdb=$dm_mobdi_tmp
#input
tmp_device_location_summary_monthly=$tmpdb.tmp_device_location_summary_monthly
tmp_device_frequency_place=$tmpdb.tmp_device_frequency_place

prepare_sql="
select device,lon as centerlon,lat as centerlat,-1 as cluster,day
from
(
   select device,lon,lat,time,unix_timestamp(concat(day, ' ', time), 'yyyyMMdd HH:mm:ss') as datetime,day
   from
   $tmp_device_location_summary_monthly
   where month between '${p3month}' and '${p1month}' and type = 4 and (station <> 'wifi' or wifi_type = '')
)a
left semi join
(select device from $tmp_device_frequency_place where stage in ('A','B','C')  group by device having count(1) < 10) b
on a.device = b.device
"

spark2-submit --master yarn \
		      --executor-memory 9G \
			  --driver-memory 15G \
			  --executor-cores 3 \
        --name "frequency D" \
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
        --conf spark.sql.adaptive.enabled=true \
         --conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=256000000 \
			  /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar D "$prepare_sql" 1
