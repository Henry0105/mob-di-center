#!/bin/bash

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

###源表
dwd_auto_location_info_sec_di=dm_mobdi_master.dwd_auto_location_info_sec_di

###映射表
dim_latlon_blacklist_mf=dm_mobdi_mapping.dim_latlon_blacklist_mf
geohash6_area_mapping_par=dm_sdk_mapping.geohash6_area_mapping_par
geohash8_lbs_info_mapping_par=dm_sdk_mapping.geohash8_lbs_info_mapping_par

###目标表
dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di

day=$1
plus_1day=`date +%Y%m%d -d "${day} +1 day"`
plus_2day=`date +%Y%m%d -d "${day} +2 day"`
echo "startday: "$day
echo "endday:   "$plus_2day

#ip_mapping_sql="
#    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
#    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
#    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_latlon_blacklist_mf', 'day');
#"
#last_ip_mapping_partition=(`hive -e "$ip_mapping_sql"`)

#获取小于当前日期的最大分区
par_arr=(`hive -e "show partitions dm_mobdi_mapping.dim_latlon_blacklist_mf" |awk -F '=' '{print $2}'|xargs`)
for par in ${par_arr[@]}
do
  if [ $par -le $day ]
  then
    last_ip_mapping_partition=$par
  else
    break
  fi
done

# check source data: #######################
CHECK_DATA()
{
  local src_path=$1
  hadoop fs -test -e $src_path
  if [[ $? -eq 0 ]] ; then
    # path存在
    src_data_du=`hadoop fs -du -s $src_path | awk '{print $1}'`
    # 文件夹大小不为0
    if [[ $src_data_du != 0 ]] ;then
      return 0
    else
      return 1
    fi
  else
      return 1
  fi
}
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_auto_location_info_sec_di/day=${day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_auto_location_info_sec_di/day=${plus_1day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_auto_location_info_sec_di/day=${plus_2day}"
# ##########################################

hive -v -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=32000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/dependencies/lib/lamfire-2.1.4.jar;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function coordConvert as 'com.youzu.mob.java.udf.CoordConvertor';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

with auto_location_info as (
  select
  nvl(muid, '') as device,
  duid,
  case when latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 then round(cast(split(coordConvert(latitude, longitude, 'wsg84', 'bd09'), ',')[0] as double), 6) else '' end as lat,  --wgs84转换为bd09
  case when latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 then round(cast(split(coordConvert(latitude, longitude, 'wsg84', 'bd09'), ',')[1] as double), 6) else '' end as lon,
  from_unixtime(CAST(clienttime/1000 as BIGINT), 'HH:mm:ss') as time,
  day as processtime,
  plat,
  networktype as network,
  'gps' as type,
  case when location_type = 1 then 'gps_auto' when location_type = 2 then 'network_auto' else 'unknown_auto' end as data_source,
  '' as orig_note1,
  '' as orig_note2,
  accuracy,apppkg,clientip as ipaddr,serdatetime,language
  from $dwd_auto_location_info_sec_di
  where day between '$day' and '$plus_2day'  --取开始日期起3天范围
  and from_unixtime(CAST(clienttime/1000 as BIGINT), 'yyyyMMdd') = '$day' --取clienttime转换为当日的数据
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
  and plat = '1'
  union all
  select
  nvl(deviceid, '') as device,
  duid,
  case when latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 then round(cast(split(coordConvert(latitude, longitude, 'wsg84', 'bd09'), ',')[0] as double), 6) else '' end as lat,  --wgs84转换为bd09
  case when latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 then round(cast(split(coordConvert(latitude, longitude, 'wsg84', 'bd09'), ',')[1] as double), 6) else '' end as lon,
  from_unixtime(CAST(clienttime/1000 as BIGINT), 'HH:mm:ss') as time,
  day as processtime,
  plat,
  networktype as network,
  'gps' as type,
  case when location_type = 1 then 'gps_auto' when location_type = 2 then 'network_auto' else 'unknown_auto' end as data_source,
  '' as orig_note1,
  '' as orig_note2,
  accuracy,apppkg,clientip as ipaddr,serdatetime,language
  from $dwd_auto_location_info_sec_di
  where day between '$day' and '$plus_2day'  --取开始日期起3天范围
  and from_unixtime(CAST(clienttime/1000 as BIGINT), 'yyyyMMdd') = '$day' --取clienttime转换为当日的数据
  and trim(lower(deviceid)) rlike '^[a-f0-9]{40}$' and trim(deviceid)!='0000000000000000000000000000000000000000'
  and plat = '2'
)
insert overwrite table $dwd_device_location_info_di partition (day='$day', source_table='auto_location_info')
select
    nvl(device,'') as device,
    nvl(duid,'') as duid,
    nvl(lat,'') as lat,
    nvl(lon,'') as lon,
    nvl(time,'') as time,
    nvl(processtime,'') as processtime,
    nvl(country,'') as country,
    nvl(province,'') as province,
    nvl(city,'') as city,
    nvl(area,'') as area,
    nvl(street,'') as street,
    nvl(plat,'') as plat,
    nvl(network,'') as network,
    nvl(type,'') as type,
    nvl(data_source,'') as data_source,
    nvl(orig_note1,'') as orig_note1,
    nvl(orig_note2,'') as orig_note2,
    nvl(accuracy,'') as accuracy,
    nvl(apppkg,'') as apppkg,
    nvl(orig_note3,'') as orig_note3,
    nvl(abnormal_flag,'') as abnormal_flag,
    nvl(ga_abnormal_flag,'') as ga_abnormal_flag
from (
    select
        trim(lower(device)) device,
        duid,
        if(a.lat is null or a.lat > 90 or a.lat < -90, '', a.lat) as lat,
        if(a.lon is null or a.lon> 180 or a.lon< -180, '', a.lon) as lon,
        time,
        processtime,
        country,
        province,
        city,
        area,
        street,
        plat,
        if(network is null or (trim(lower(network)) not rlike '^(2g)|(3g)|(4g)|(5g)|(cell)|(wifi)|(bluetooth)$'),'',trim(lower(network))) as network,
        type,
        data_source,
        orig_note1,
        orig_note2,
        accuracy,
        if(apppkg is null or trim(apppkg) in ('null','NULL') or trim(apppkg)!=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0),'',trim(apppkg)) as apppkg,
        orig_note3,
        case when b.lat is null and b.lon is null then 0 else 1 end as abnormal_flag,
        0 as ga_abnormal_flag
    from (
        select
        device, duid,
        coalesce(lat, '') as lat,
        coalesce(lon, '') as lon,
        time, processtime,
        substr(coalesce(geo6.province, geohash8_mapping.province_code, ''), 1, 2) as country,
        coalesce(geo6.province, geohash8_mapping.province_code, '') as province,
        coalesce(geo6.city, geohash8_mapping.city_code, '') as city,
        coalesce(geo6.area, geohash8_mapping.area_code, '') as area,
        '' as street,
        plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg, '' as orig_note3,ipaddr,serdatetime,language
        from (
            select
                device, duid, lat, lon, time, processtime,
                geohash6_mapping.province_code as province,
                geohash6_mapping.city_code as city,
                geohash6_mapping.area_code as area,
                geohash6_mapping.geohash_6_code,
                plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg,ipaddr,serdatetime,language
            from auto_location_info log
            left join (
                select * from $geohash6_area_mapping_par
                where version='1000'
            ) geohash6_mapping
            on (get_geohash(lat, lon, 6) = geohash6_mapping.geohash_6_code)  --通过GEOHASH6关联
        ) geo6
        left join (
            select * from $geohash8_lbs_info_mapping_par
            where version='1000'
        ) geohash8_mapping
        on (case when geo6.geohash_6_code is null then get_geohash(lat, lon, 8) else concat('', rand()) end = geohash8_mapping.geohash_8_code) --未关联上的通过GEOHASH8关联
    ) a
    left join (
        select lat,lon from $dim_latlon_blacklist_mf
        where day='$last_ip_mapping_partition'
          and stage='A'
    ) b
    on round(a.lat,5)=round(b.lat,5) and round(a.lon,5)=round(b.lon,5)
)a
group by  device,duid,lat,lon,time,processtime,country,province,city,area,street,plat,network,type,data_source,orig_note1,orig_note2,accuracy,apppkg,orig_note3,abnormal_flag,ga_abnormal_flag
;
"
