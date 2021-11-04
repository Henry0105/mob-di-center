#!/bin/bash

set -e -x

: '
@owner:luost
@describe:线下母婴人群圈定
@projectName:
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dim_poi_config_mapping_par=dim_sdk_mapping.dim_poi_config_mapping_par
#dm_sdk_mapping.poi_config_mapping_par
#dm_mobdi_topic.dws_device_location_staying_di
#tmp
offline_chile_mom=$dm_mobdi_tmp.offline_chile_mom
#out
#dm_mobdi_report.timewindow_offline_profile_v2

day=$1

#通过poi通用工具计算device_staying_daily中数据
spark2-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--driver-memory 4g \
--executor-memory 8G \
--executor-cores 2 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=100 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=500 \
--conf spark.sql.shuffle.partitions=500 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar \
 "{
     \"dataType\": \"1\",
     \"lbsSql\": \"select device,lat,lon from $dws_device_location_staying_di where day='$day' and type in ('gps','wifi','tlocation','base') and accuracy != '0'\",
     \"poiTable\": \"(select name,lat,lon,attribute,baidu_lat_lon_boundary,type from $dim_poi_config_mapping_par where version = '1001' and type = 26 and get_json_object(attribute,'$.cate2') = '妇幼保健')\",
     \"poiFields\": \"name,lat,lon,attribute,baidu_lat_lon_boundary,type\",
     \"poiCalFields\": {
         \"distance\": {
             \"distanceRange\": \"100\"
         }
     },
     \"poiCondition\": {
         \"type\": \"26\"
     },
     \"targetTable\": \"$offline_chile_mom\"
 }"


#将device写入timewindow_offline_profile_v2
hive -v -e "
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_offline_profile_v2 partition (flag='26',day = '$day',timewindow = '1')
select device,'category2' as feature,'妇幼保健=1' as cnt
from $offline_chile_mom
group by device;
"
