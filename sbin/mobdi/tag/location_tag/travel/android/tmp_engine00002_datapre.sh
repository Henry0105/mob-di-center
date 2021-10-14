#!/bin/bash


set -e -x

source ../../../../util/util.sh

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

location_day=${day:0:6}01

day_run=`date +%Y%m%d -d "${location_day} -1 month"`

p30=`date +%Y%m%d -d "${day_run} -1 month"`


source /home/dba/mobdi_center/conf/hive-env.sh


## 源表
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di
#rp_device_location_3monthly=rp_mobdi_app.rp_device_location_3monthly

## mapping表
#dim_bssid_type_mf=dim_mobdi_mapping.dim_bssid_type_mf
#dim_bssid_type_all_mf=dm_mobdi_mapping.dim_bssid_type_all_mf
#dim_geohash6_china_area_mapping_par=dim_sdk_mapping.dim_geohash6_china_area_mapping_par
#dim_geohash8_china_area_mapping_par=dim_sdk_mapping.dim_geohash8_china_area_mapping_par
#dim_geohash6_foreign_mapping_par=dm_sdk_mapping.dim_geohash6_foreign_mapping_par

dim_bssid_type_mf_db=${dim_bssid_type_mf%.*}
dim_bssid_type_mf_tb=${dim_bssid_type_mf#*.}
rp_device_location_3monthly_db=${rp_device_location_3monthly%.*}
rp_device_location_3monthly_tb=${rp_device_location_3monthly#*.}

## 目标表
tmp_engine00002_datapre=$dm_mobdi_tmp.tmp_engine00002_datapre

## 获取最新分区

sql1="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$dim_bssid_type_mf_db', '$dim_bssid_type_mf_tb', 'day');
drop temporary function GET_LAST_PARTITION;
"
dim_bssid_type_all_mf_lastday=(`hive  -e "$sql1"`)

sql2="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$rp_device_location_3monthly_db', '$rp_device_location_3monthly_tb', 'day');
drop temporary function GET_LAST_PARTITION;
"

## 补数据防止出现不需要最新分区的情况
rp_device_location_3monthly_lastday=(`hive -e "$sql2"`)
location_day=${day:0:6}01


if [ ${location_day} -gt ${rp_device_location_3monthly_lastday} ]; then
location_day=${rp_device_location_3monthly_lastday}
fi


sql_final="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';

with gps_process
as (
    select device, lat, lon, starttime, endtime, country, province, city, day,orig_note1,
    lag(lat, 1) over (partition by device order by starttime, endtime) as lat_new,
    lag(lon, 1) over (partition by device order by starttime, endtime) as lon_new,
    lag(starttime, 1) over (partition by device order by starttime, endtime) as starttime_new,
    lag(endtime, 1) over (partition by device order by starttime, endtime) as endtime_new
    from (
       select a1.device,a1.lat,a1.lon,a1.country,province, city, a1.day,orig_note1,starttime,endtime
           from(
                select device, lat, lon,country,province, city, day,orig_note1, unix_timestamp(concat (day, ' ', start_time),
                        'yyyyMMdd HH:mm:ss') as starttime, unix_timestamp(concat (day, ' ', end_time),
                        'yyyyMMdd HH:mm:ss') as endtime
                from $dws_device_location_staying_di
                where day >= '$p30' and day < '$day_run' and abnormal_flag = 0 and type = 'gps'
               ) a1
           inner join(
                select *
                from (
                    select lat, lon, day,count(1) as cnt
                    from $dws_device_location_staying_di
                    where lat is not null and lon is not null   -- 去除 这个条件，应该是写错了： and day = '${day_run}'
                    and day >= '$p30' and day < '$day_run' and abnormal_flag = 0 and type = 'gps'
                    group by lat, lon,day
                    ) tt
                where cnt <= 5
                     ) a2
           on a1.lat=a2.lat and a1.lon=a2.lon and a1.day=a2.day
         ) t1
    ),
travel_pre
as (
    select device, lat, lon, city, day, lat_new, lon_new,
           cast(get_distance(lat1, lon1, lat2, lon2) as int) as distance_diff,
           starttime - starttime_new + 0.1 as time_diff
    from (
        select device, lat, lon, city, day, lat_new, lon_new, starttime_new, starttime,
        coalesce(lat, lat_new, 0) lat1, coalesce(lon, lon_new, 0) lon1,
        coalesce(lat_new, lat, 0) lat2, coalesce(lon_new, lon, 0) lon2
        from gps_process
        ) a
    ),
gps_in
as (
    select t1.device as device, t1.lat, t1.lon,t1.orig_note1, starttime, endtime,t1.country, t1.province, t1.city, t1.day
    from gps_process t1
    inner join (
        select *
        from (
            select device, lat, lon, count(1) as num
            from (
                select device, lat, lon
                from travel_pre
                where time_diff <= 43200
                and distance_diff >= 100 and cast(nvl(distance_diff / time_diff, 0) as bigint) >= 30

                union all

                select device, lat_new as lat, lon_new as lon
                from travel_pre
                where time_diff <= 43200 and distance_diff >= 100 and cast(nvl(distance_diff / time_diff, 0) as bigint) >=30
                ) a
            group by device, lat, lon
            ) tt
        where num >= 2
        ) t2
    on t1.device = t2.device and t1.lat = t2.lat and t1.lon = t2.lon
    where t2.device is null
    )

insert overwrite table $tmp_engine00002_datapre partition(day='$day')
select t1.device as device, t1.lat, t1.lon, t1.starttime, t1.endtime,
      case when t1.province is null or length(t1.province)<=0 then
       if(geohash6_mapping.province_code is not null or geohash8_mapping.province_code is not null,'cn',foreign_mapping.country_code)
       else t1.country end as country,
       case when t1.province is null or length(t1.province)<=0 then
       if(geohash6_mapping.province_code is not null or geohash8_mapping.province_code is not null,coalesce(geohash6_mapping.province_code,geohash8_mapping.province_code),foreign_mapping.province_code)
       else t1.province end as province,
       case when t1.province is null or length(t1.province)<=0 then
       if(geohash6_mapping.province_code is not null or geohash8_mapping.province_code is not null,coalesce(geohash6_mapping.city_code,geohash8_mapping.city_code),foreign_mapping.city_code)
       else t1.city end as city,
       t1.day,
       t1.orig_note1,city_home, city_work
from (
    select device, lat, lon, starttime, endtime,country, province, city, day ,orig_note1
    from gps_in

    union all

    select device, lat, lon, starttime, endtime,country, province, city, day ,orig_note1
    from (
          select device, lat, lon,
                 unix_timestamp(concat (day, ' ', start_time),'yyyyMMdd HH:mm:ss') as starttime,
                 unix_timestamp(concat (day, ' ', end_time),'yyyyMMdd HH:mm:ss') as endtime,
                 orig_note1, city, day,country,province
          from $dws_device_location_staying_di
          where abnormal_flag = 0 and type = 'wifi' and orig_note1 is not null and day >= '$p30' and day < '$day_run'
        ) t1
    inner join
       (
        select bssid
        from $dim_bssid_type_mf
        where day = '$dim_bssid_type_all_mf_lastday' and type = 1
        ) t2
    on regexp_replace(t1.orig_note1, 'bssid=', '') = t2.bssid

    union all

    select device, lat, lon, unix_timestamp(concat (day, ' ', start_time),'yyyyMMdd HH:mm:ss') as starttime,
           unix_timestamp(concat (day, ' ', end_time),'yyyyMMdd HH:mm:ss') as endtime,
           country, province, city, day ,orig_note1
    from $dws_device_location_staying_di
    where abnormal_flag = 0 and type = 'base' and day >= '$p30' and day < '$day_run'

    union all

    select device, lat, lon, unix_timestamp(concat (day, ' ', start_time),'yyyyMMdd HH:mm:ss') as starttime,
    unix_timestamp(concat (day, ' ', end_time),'yyyyMMdd HH:mm:ss') as endtime,
    country, province, city, day ,orig_note1
    from $dws_device_location_staying_di
    where abnormal_flag = 0 and type = 't_location' and day >= '$p30' and day < '$day_run'

   union all

    select device, lat, lon, unix_timestamp(concat (day, ' ', start_time),
                'yyyyMMdd HH:mm:ss') as starttime, unix_timestamp(concat (day, ' ', end_time),
                'yyyyMMdd HH:mm:ss') as endtime,country, province, city, day ,orig_note1
    from $dws_device_location_staying_di
    where type = 'wifi_scan_list' and day >= '$p30' and day < '$day_run'
    ) t1
left join (
    select device,
           case when confidence_home >= 0.7 then city_home else '' end city_home,
           case when confidence_work >= 0.7 then city_work else '' end city_work
    from $rp_device_location_3monthly
    where day = '$location_day' and (confidence_home >= 0.7 or confidence_work >= 0.7)
    ) t2 on t1.device = t2.device
left join (
     select * from $dim_geohash6_china_area_mapping_par where version='1000') geohash6_mapping
     on (get_geohash(lat, lon, 6) = geohash6_mapping.geohash_6_code) --根据geohash6关联
left join (
     select * from $dim_geohash8_china_area_mapping_par where version='1000') geohash8_mapping
     on (get_geohash(lat, lon, 8) = geohash8_mapping.geohash_8_code)  --未关联上的再根据geohash8关联
left join (
     select * from $dim_geohash6_foreign_mapping_par where version='1000') foreign_mapping
     on (get_geohash(lat,lon,6)=foreign_mapping.geohash_6_code);
"
hive_setting "$sql_final"