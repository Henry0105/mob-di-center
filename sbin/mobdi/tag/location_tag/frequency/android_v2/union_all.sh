#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01

source /home/dba/mobdi_center/conf/hive-env.sh

#input
tmp_device_frequency_place=dm_mobdi_tmp.tmp_device_frequency_place

#mapping
#dim_geohash6_china_area_mapping_par=dim_sdk_mapping.dim_geohash6_china_area_mapping_par
#dm_sdk_mapping.geohash6_area_mapping_par
#dim_geohash8_china_area_mapping_par=dim_sdk_mapping.dim_geohash8_china_area_mapping_par
#dm_sdk_mapping.dim_geohash8_china_area_mapping_par

#out
#dm_mobdi_report.rp_device_frequency_3monthly

hive -e"

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
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

insert overwrite table $rp_device_frequency_3monthly partition(day='${days}')
 select
       device,
       lon,
       lat,
       cnt,
       substr(coalesce(geo6.province, geohash8_mapping.province_code, ''), 1, 2) as country,
       coalesce(geo6.province, geohash8_mapping.province_code, '') as province,
       coalesce(geo6.city,geohash8_mapping.city_code,'') as city,
       coalesce(geo6.area, geohash8_mapping.area_code, '') as area,
       rank,
       confidence
    from
    (
    select
       device,
       centerlon AS lon,
       centerlat AS lat,
       cnt_active AS cnt,
       confidence,
       geohash6_mapping.province_code as province,
       geohash6_mapping.area_code as area,
       geohash6_mapping.city_code as city,
       geohash6_mapping.geohash_6_code,
       rk as rank
    from
    (
      select *,row_number() over(partition by device order by confidence desc) as rk
      from $tmp_device_frequency_place
      where stage in('A','B','C','D')
    ) frequency
    left join (select * from $dim_geohash6_china_area_mapping_par where version='1000') geohash6_mapping
    on (get_geohash(centerlat, centerlon, 6) = geohash6_mapping.geohash_6_code)
    where frequency.rk <= 10
    )geo6
    left join (select * from $dim_geohash8_china_area_mapping_par where version='1000') geohash8_mapping
    on (case when geo6.geohash_6_code is null then get_geohash(lat, lon, 8) else concat('', rand()) end = geohash8_mapping.geohash_8_code)
"