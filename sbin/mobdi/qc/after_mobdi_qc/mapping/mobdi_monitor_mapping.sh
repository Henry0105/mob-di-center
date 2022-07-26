#!/bin/bash

set -x -e

: '
@owner:liuyanqiang
@describe:监控mapping表100x分区是否与最新分区一致
'

mapping="dm_sdk_mapping.app_category_mapping_par,"\
"dm_sdk_mapping.app_pkg_mapping_par,"\
"dm_sdk_mapping.app_tag_system_mapping_par,"\
"dm_sdk_mapping.brand_model_mapping_par,"\
"dm_sdk_mapping.cate_id_mapping_par,"\
"dm_sdk_mapping.game_app_detail_par,"\
"dm_sdk_mapping.game_ip_id_mapping_par,"\
"dm_sdk_mapping.game_network_frame_id_mapping_par,"\
"dm_sdk_mapping.game_style_id_mapping_par,"\
"dm_sdk_mapping.geohash6_area_mapping_par,"\
"dm_sdk_mapping.geohash8_lbs_info_mapping_par,"\
"dm_sdk_mapping.mapping_city_level_par,"\
"dm_sdk_mapping.p2p_app_cat_par,"\
"dm_sdk_mapping.poi_config_mapping_par,"\
"dm_sdk_mapping.tag_cat_mapping_dmp_par,"\
"dm_sdk_mapping.tag_id_mapping_par,"\
"dm_mobdi_mapping.bitauto_brand_info_par,"\
"dm_mobdi_mapping.mapping_ios_factory_par"

sendMail="DIMonitor@mob.com"

spark2-submit --master yarn --deploy-mode cluster \
--class com.mob.mobdi.utils.MonitorMapping \
--driver-memory 12G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.driver.maxResultSize=4g \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/lib/MobDI_Monitor-1.0-SNAPSHOT-jar-with-dependencies.jar "$mapping" "$sendMail"