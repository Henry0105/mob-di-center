#!/bin/sh
set -e -x
: '
@owner:zhangxy
@describe:计算device所在经纬度附近多种行业类型的poi信息
@projectName:MobDI
@BusinessName:lbs_poi
@SourceTable:dm_sdk_mapping.poi_config_mapping_par,dm_mobdi_topic.dws_device_location_staying_di,dm_mobdi_tmp.lbs_poi_mapping_catering
@TargetTable:dm_mobdi_master.sdk_lbs_daily_poi
@TableRelation:dm_mobdi_topic.dws_device_location_staying_di,dm_sdk_mapping.poi_config_mapping_par->dm_mobdi_tmp.lbs_poi_mapping_catering|dm_mobdi_tmp.lbs_poi_mapping_catering->dm_mobdi_topic.dws_device_lbs_poi_android_sec_di
'
if [ $# -lt 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>,<type>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

#tmp
lbs_poi_mapping_catering=$dm_mobdi_tmp.lbs_poi_mapping_catering

cd `dirname $0`
day=$1
type=$2


spark2-submit  --master yarn --deploy-mode cluster \
 --class com.youzu.mob.poi.PoiExport \
 --conf spark.shuffle.service.enabled=true \
 --conf spark.dynamicAllocation.enabled=true \
 --conf spark.dynamicAllocation.minExecutors=100 \
 --conf spark.dynamicAllocation.maxExecutors=400 \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 --driver-java-options "-XX:MaxPermSize=1g" \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar \
 "{
    \"dataType\": \"1\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiTable\": \"(select poi_id,name,lat,lon,geohash6,geohash7,attribute,type from(select poi_id,name,lat,lon,geohash6,geohash7,attribute,type,row_number() over(partition by lat,lon order by poi_id)as rank from $poi_config_mapping_par where version='1001' and  type in (${type}) and get_json_object(attribute, '$.cat1') <>'')t where rank=1)\",
    \"lbsSql\": \"select device,start_time as begintime,end_time as endtime,
lat,
lon,
wgs84point2geohashUDF(lat,lon,'8') as geohash7
from $dws_device_location_staying_di
where day=${day} and data_source <> 'pv' and type <> 'ip' and plat = 1 and start_time <= end_time\",
    \"poiFields\": \"poi_id,name,lat,lon,geohash6,geohash7,type\",
    \"poiCondition\": {
        \"type\": \"${type}\"
    },
    \"targetTable\": \"$lbs_poi_mapping_catering\"
}"

hive -e"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table $dws_device_lbs_poi_android_sec_di partition(type,day)
  select a.device,a.lat,a.lon,c.net_type,a.begintime,a.endtime,c.country,
       c.province,c.city,c.area,c.street,c.plat,a.geohash7 as geohash,a.poiInfo,a.poi_type as type,c.day
 from $lbs_poi_mapping_catering a
 inner join 
 (
 select device,lat,lon,type as net_type,start_time as begintime,end_time as endtime,plat,country,province,city,area,street,day
 from $dws_device_location_staying_di
 where day=${day} and data_source <> 'pv' and type <> 'ip' and plat = 1
 )c
 on a.device=c.device and a.lat=c.lat and a.lon=c.lon and a.begintime=c.begintime and a.endtime=c.endtime
"

