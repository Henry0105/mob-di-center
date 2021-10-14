#!/bin/sh

set -e -x

: '
@owner:zhoup
@describe:计算ios所在经纬度附近多种行业类型的poi信息
@projectName:MobDI
@BusinessName:lbs_poi
@SourceTable:dm_sdk_mapping.poi_config_mapping_par,dm_mobdi_master.device_staying_daily,dm_mobdi_mapping.ios_id_mapping_full_view,dw_mobdi_md.lbs_poi_${type}_tmp
@TargetTable:dm_mobdi_master.sdk_lbs_daily_poi
@TableRelation:dm_mobdi_master.device_staying_daily,dm_sdk_mapping.poi_config_mapping_par->dm_sdk_master.sdk_lbs_daily_poi
@update:波纹项目将库表名重新定义，表依赖关系看下面注释
'

if [ $# -ne 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <type>,<date>"
    exit 1
fi

#入参
day=$1
type=$2

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di
#dim_id_mapping_ios_sec_df_view=dim_mobdi_mapping.dim_id_mapping_ios_sec_df_view

#mapping
#dim_poi_config_mapping_par=dim_sdk_mapping.dim_poi_config_mapping_par
#poi_config_mapping_par=dm_sdk_mapping.poi_config_mapping_par

#md
#lbs_poi_3_tmp=dm_mobdi_topic.dws_lbs_poi_3_di
#lbs_poi_6_tmp=dm_mobdi_topic.dws_lbs_poi_3_di
#lbs_poi_9_tmp=dm_mobdi_topic.dws_lbs_poi_3_di

## 目标表
#dws_ifid_lbs_poi_ios_sec_di=dm_mobdi_topic.dws_ifid_lbs_poi_ios_sec_di

spark2-submit \
 --master yarn \
 --deploy-mode client \
 --class com.youzu.mob.poi.PoiExport \
 --conf spark.shuffle.service.enabled=true\
 --conf spark.dynamicAllocation.enabled=true\
 --conf spark.dynamicAllocation.minExecutors=100\
 --conf spark.dynamicAllocation.maxExecutors=400\
 /home/dba/lib/mobdi-poi-tool-v0.1.0.jar \
 "{
    \"dataType\": \"1\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiTable\": \"(select poi_id,name,lat,lon,bssid,geohash6,geohash7,attribute,type from $dim_poi_config_mapping_par where version='1000')t\",
    \"lbsSql\": \"select ifid,lat,lon,net_type,begintime,endtime,country,province,city,area,street,plat,geohash7,bssid,day
from
(
  select c.ifid,b.lat,b.lon,b.type as net_type,b.begintime,b.endtime,b.country,
         b.province,
         b.city,
         b.area,
         b.street,
         b.plat,
         b.geohash as geohash7,
         case when split(b.orig_note1,'=')[0] ='bssid' then split(b.orig_note1,'=')[1] else '' end as bssid,
         b.day
  from
  (
      select device,lat,lon,
             type,start_time as begintime,
             end_time as endtime,
             plat,country,province,city,area,street,
             wgs84point2geohashUDF(lat,lon,'8') as geohash,
             orig_note1,
             day
      from $dws_device_location_staying_di t
      where day=${day}
      and plat=2
      and data_source <> 'pv'
      and type <> 'ip'
      and start_time <= end_time
  ) b
  join
  (
      select device,
             ifids as ifid
      from $dim_id_mapping_ios_sec_df_view
      lateral view explode(split(ifid,',')) t as ifids
      where ifids <> ''
  ) c
  on b.device=c.device
) ff \",
    \"poiFields\": \"poi_id,name,lat,lon,bssid,geohash6,geohash7,type\",
    \"poiCondition\": {
        \"type\": \"${type}\"
    },
    \"targetTable\": \"$dm_mobdi_topic.lbs_poi_${type}_tmp\"
}"

hive -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $dws_ifid_lbs_poi_ios_sec_di partition(day,type)
select ifid,
       lat,
       lon,
       net_type,
       begintime,
       endtime,
       country,
       province,
       city,
       area,
       street,
       plat,
       geohash7 as geohash,
       poiinfo,
       poi_type as type,
       day
from $dm_mobdi_topic.lbs_poi_${type}_tmp
where day=${day}
cluster by ifid;
"
