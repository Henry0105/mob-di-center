#!/bin/sh
: '
@owner:zhoup
@describe:ios常驻地计算
@projectName:
@BusinessName:
@SourceTable:dm_mobdi_master.device_ip_info,dm_mobdi_mapping.ios_id_mapping_full_view,dm_sdk_mapping.map_city_sdk
@TargetTable:rp_mobdi_app.ios_permanent_place
@TableRelation:dm_sdk_mapping.map_city_sdk,dm_mobdi_master.device_ip_info,dm_mobdi_mapping.ios_id_mapping_full_view->rp_mobdi_app.ios_permanent_place
'
set -x -e
day=$1
p90day=`date -d "$day -30 days" +%Y%m%d`
spark2-submit --master yarn \
			  --executor-memory 9G \
			  --driver-memory 15G \
			  --executor-cores 3  \
			  --class com.youzu.mob.permanent.IosPermanentPlace \
			  --conf spark.dynamicAllocation.enabled=true \
			  --conf spark.dynamicAllocation.minExecutors=60\
			  --conf spark.dynamicAllocation.maxExecutors=100 \
			  --conf spark.default.parallelism=1200 \
			  --conf spark.sql.shuffle.partitions=1500 \
			  /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $day $p90day
			  
#~/jdk1.8.0_45/bin/java -cp /home/dba/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d rp_mobdi_app -t ios_permanent_place
