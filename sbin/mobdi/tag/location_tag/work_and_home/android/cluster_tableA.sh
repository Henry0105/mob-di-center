#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01

p3month=`date -d "$days -3 month" +%Y%m`

p1month=`date -d "$days -1 month" +%Y%m`
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
#input
tmp_device_location_summary_monthly=$tmpdb.tmp_device_location_summary_monthly

#out
tmp_device_location_stage_pre=$tmpdb.tmp_device_location_stage_pre
tmp_device_dbscan_result=$tmpdb.tmp_device_dbscan_result
tmp_device_work_place=$tmpdb.tmp_device_work_place
tmp_device_live_place=$tmpdb.tmp_device_live_place
tmp_device_location_cluster_rank=$tmpdb.tmp_device_location_cluster_rank


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

insert overwrite table $tmp_device_location_stage_pre partition(stage='A')
select device,lon,lat,time,unix_timestamp(concat(day, ' ', time), 'yyyyMMdd HH:mm:ss') as datetime,day
from
$tmp_device_location_summary_monthly
where month between '${p3month}' and '${p1month}' and type = 1
and lon not in('','0.0','0.0065') and lat not in ('','0.0','0.006')
"

submit(){
i=$1
prepare_sql="
select * from $tmp_device_location_stage_pre
where stage='A'  and cast(conv(substring(device,0,1), 16, 10) as int) & 3 = $i
"
spark2-submit --master yarn \
		      --executor-memory 10G \
			  --driver-memory 10G \
        --executor-cores 3 \
        --name "dbscan 3monthly table A$i" \
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
        /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "A${i}" "${prepare_sql}" 1
}

for i in {0..3}
do
submit ${i}
done

# 汇总
hive -e"
insert overwrite table $tmp_device_dbscan_result partition(stage ='A')
select device,lon,lat,cluster,centerlon,centerlat,distance,day,hour,1 as weight
from $tmp_device_dbscan_result where stage in('A0','A1','A2','A3')
"

hive -e"
alter table $tmp_device_dbscan_result drop partition(stage='A0');
alter table $tmp_device_dbscan_result drop partition(stage='A1');
alter table $tmp_device_dbscan_result drop partition(stage='A2');
alter table $tmp_device_dbscan_result drop partition(stage='A3');
"

hive -e"
insert overwrite table  $tmp_device_work_place partition(stage ='A')
select
device             ,
cluster            ,
centerlon          ,
centerlat          ,
work_num1          ,
living_num1        ,
distance_max       ,
distance_min       ,
confidence
from $tmp_device_work_place
where stage in('A0','A1','A2','A3')
"

hive -e"
alter table $tmp_device_work_place drop partition(stage='A0');
alter table $tmp_device_work_place drop partition(stage='A1');
alter table $tmp_device_work_place drop partition(stage='A2');
alter table $tmp_device_work_place drop partition(stage='A3');
"

hive -e"
insert overwrite table $tmp_device_live_place partition(stage ='A')
select
device             ,
cluster            ,
centerlon          ,
centerlat          ,
work_num1          ,
living_num1        ,
distance_max       ,
distance_min       ,
confidence
from $tmp_device_live_place
where stage in('A0','A1','A2','A3')
"
hive -e"
alter table $tmp_device_live_place drop partition(stage='A0');
alter table $tmp_device_live_place drop partition(stage='A1');
alter table $tmp_device_live_place drop partition(stage='A2');
alter table $tmp_device_live_place drop partition(stage='A3');
"

hive -e"
insert overwrite table $tmp_device_location_cluster_rank partition(stage='A')
select
device             ,
cluster            ,
centerlon          ,
centerlat          ,
total_num          ,
work_num1          ,
living_num1        ,
work_num2          ,
living_num2        ,
cluster_num        ,
all_num            ,
rate_all           ,
rate_work_all      ,
rate_work_all_max  ,
rate_living_all    ,
rate_living_all_max,
total_days         ,
work_days1         ,
work_day_flag      ,
live_days1         ,
live_day_flag      ,
work_days2         ,
live_days2         ,
distance_max       ,
distance_min       ,
work_rank          ,
live_rank
from
$tmp_device_location_cluster_rank
where stage in('A0','A1','A2','A3')
"

hive -e"
alter table $tmp_device_location_cluster_rank drop partition(stage='A0');
alter table $tmp_device_location_cluster_rank drop partition(stage='A1');
alter table $tmp_device_location_cluster_rank drop partition(stage='A2');
alter table $tmp_device_location_cluster_rank drop partition(stage='A3');
"