#!/bin/sh
: '
@owner:zhoup
@describe:ios poi信息
@projectName:
@BusinessName:
@SourceTable:dm_sdk_mapping.poi_config_mapping,dm_mobdi_mapping.ios_id_mapping_full_view,dm_sdk_master.sdk_lbs_daily_full,dm_sdk_master.sdk_lbs_daily_poi_ios
@TargetTable:dm_sdk_master.sdk_lbs_daily_poi_ios,rp_mobdi_app.timewindow_offline_profile_ios_sec
@TableRelation:dm_sdk_mapping.poi_config_mapping,dm_mobdi_mapping.ios_id_mapping_full_view,dm_sdk_master.sdk_lbs_daily_full->dm_sdk_master.sdk_lbs_daily_poi_ios|dm_sdk_master.sdk_lbs_daily_poi_ios->rp_mobdi_app.timewindow_offline_profile_ios
'
set -e -x

source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
#dws_ifid_lbs_poi_ios_sec_di=dm_mobdi_topic.dws_ifid_lbs_poi_ios_sec_di

## 目标表
#timewindow_offline_profile_ios_sec=rp_mobdi_app.timewindow_offline_profile_ios_sec


day=$1
windowTime=$2
lbstype=$3
field=$4
spark2-submit --master yarn --deploy-mode client \
--class com.youzu.mob.tools.IosOfflineUniversalToolsSec \
--driver-memory 10G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=3 \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.3 \
--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-java-options "-XX:MaxPermSize=1024m" \
/home/dba/mobdi_center/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar \
"
{
    \"partition\": \"${day}\",
    \"fileNum\": \"100\",
    \"field\": \"${field}\",
    \"lbstype\": \"${lbstype}\",
    \"windowTime\": \"${windowTime}\"
}
"
#~/jdk1.8.0_45/bin/java -cp /home/dba/mobdi_center/lib/mysql-utils-1.0-jar-with-dependencies.jar  com.mob.TagUpdateTime -d sec_mobdi_test -t timewindow_offline_profile_ios_sec