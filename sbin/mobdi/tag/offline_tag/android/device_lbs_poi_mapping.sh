#!/bin/sh
set -e -x
: '
@owner:xdzhang
@describe:计算device所在经纬度附近多种行业类型的poi信息
@projectName:MobDI
@BusinessName:lbs_poi
@SourceTable:dm_sdk_mapping.poi_config_mapping_par,dm_mobdi_topic.dws_device_location_staying_di,dm_mobdi_tmp.lbs_poi_mapping_tmp

@TargetTable:dm_mobdi_topic.dws_device_lbs_poi_android_sec_di
@TableRelation:dm_mobdi_topic.dws_device_location_staying_di,dm_sdk_mapping.poi_config_mapping_par->dm_mobdi_tmp.lbs_poi_mapping_tmp|dm_mobdi_tmp.lbs_poi_mapping_tmp->dm_mobdi_topic.dws_device_lbs_poi_android_sec_di
'
if [ $# -lt 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <type>,<date>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

#tmp
lbs_poi_mapping_tmp=$dm_mobdi_tmp.lbs_poi_mapping_tmp
#dim_poi_config_mapping_par=dim_sdk_mapping.dim_poi_config_mapping_par
poi_config_db=${dim_poi_config_mapping_par%.*}
poi_config_tb=${dim_poi_config_mapping_par#*.}

cd `dirname $0`
type=$2
day=$1

sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$poi_config_db', '$poi_config_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
lastpar=(`hive -e "$sql"`)

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
    \"poiTable\": \"(select poi_id,name,lat,lon,bssid,geohash6,geohash7,attribute,type from $dim_poi_config_mapping_par where version='1000')\",
    \"lbsSql\": \"select device,start_time as begintime,end_time as endtime,
lat ,
lon,
wgs84point2geohashUDF(lat,lon,'8') as geohash7
from $dws_device_location_staying_di
where day=${day} and plat = 1 and  data_source <> 'pv' and type <> 'ip' and start_time <= end_time\",
    \"poiFields\": \"poi_id,name,lat,lon,bssid,geohash6,geohash7,type\",
    \"poiCondition\": {
        \"type\": \"${type}\"
    },
    \"targetTable\": \"$lbs_poi_mapping_tmp\"
}"

hive -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table $dws_device_lbs_poi_android_sec_di partition(type,day)
  select a.device,a.lat,a.lon,c.net_type,a.begintime,a.endtime,c.country,
       c.province,c.city,c.area,c.street,c.plat,a.geohash7 as geohash,a.poiInfo,a.poi_type as type,c.day
 from $lbs_poi_mapping_tmp a
 inner join
 (
 select device,lat,lon,type as net_type,start_time as begintime,end_time as endtime,plat,country,province,city,area,street,day
 from $dws_device_location_staying_di
 where day=${day} and plat = 1
 )c
 on a.device=c.device and a.lat=c.lat and a.lon=c.lon and a.begintime=c.begintime and a.endtime=c.endtime
"

