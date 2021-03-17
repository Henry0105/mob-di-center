#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01

source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
tmp_device_live_place=dm_mobdi_tmp.tmp_device_live_place
tmp_device_work_place=dm_mobdi_tmp.tmp_device_work_place

#mapping
#dm_sdk_mapping.geohash6_area_mapping_par
#dm_sdk_mapping.geohash8_lbs_info_mapping_par
#dm_sdk_mapping.mapping_area_par

#out
#dm_mobdi_report.rp_device_location_3monthly

hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.GetDistance';
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
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



with live_place as(
  select
     device,
     lon_home,
     lat_home,
     cluster_home,
     cnt_home,
     max_distance,
     min_distance,
     confidence,
     t.country,
     t.province,
     t.city,
     t.area,
     mapping_country.country as country_cn,
     mapping_province.province as province_cn,
     mapping_city.city as city_cn,
     mapping_area.area_poi as area_cn
  from
  (
    select
       device,
       lon_home,
       lat_home,
       cluster_home,
       cnt_home,
       max_distance,
       min_distance,
       confidence,
       substr(coalesce(geo6.province, geohash8_mapping.province_code, ''), 1, 2) as country,
       coalesce(geo6.province, geohash8_mapping.province_code, '') as province,
       coalesce(geo6.city,geohash8_mapping.city_code,'') as city,
       coalesce(geo6.area, geohash8_mapping.area_code, '') as area
    from
    (
    select
       device,
       centerlon AS lon_home,
       centerlat AS lat_home,
       cluster AS cluster_home,
       living_num1 as cnt_home,
       distance_max as max_distance,
       distance_min as min_distance,
       confidence,
       geohash6_mapping.province_code as province,
       geohash6_mapping.area_code as area,
       geohash6_mapping.city_code as city,
       geohash6_mapping.geohash_6_code
    from
    (
      select *,row_number() over(partition by device order by confidence desc) as rk
      from $tmp_device_live_place
      where stage in('A','B','C','D')
    ) live
    left join (select * from $geohash6_area_mapping_par where version='1000') geohash6_mapping
    on (get_geohash(centerlat, centerlon, 6) = geohash6_mapping.geohash_6_code)
    where live.rk = 1
    )geo6
    left join (select * from $geohash8_lbs_info_mapping_par where version='1000') geohash8_mapping
    on (case when geo6.geohash_6_code is null then get_geohash(lat_home, lon_home, 8) else concat('', rand()) end = geohash8_mapping.geohash_8_code)
  )t
  left  join
  (select country,country_code from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par') group by country,country_code) mapping_country
  on t.country=mapping_country.country_code
  left join
  (select province,province_code from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par') group by province,province_code) mapping_province
  on t.province=mapping_province.province_code
  left join
  (select city,city_code from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par') group by city,city_code) mapping_city
  on t.city=mapping_city.city_code
  left join
  (
   select area_poi,area_code
   from
   (
    select area_poi,area_code,row_number() over(partition by area_code order by length(area_poi)) as rank
    from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par')
   ) min_area
   where rank=1
  ) mapping_area
  on t.area=mapping_area.area_code
),
work_place as (
  select
     device,
     lon_work,
     lat_work,
     cluster_work,
     cnt_work,
     max_distance,
     min_distance,
     confidence,
     t.country,
     t.province,
     t.city,
     t.area,
     mapping_country.country as country_cn,
     mapping_province.province as province_cn,
     mapping_city.city as city_cn,
     mapping_area.area_poi as area_cn
  from
  (
    select
       device,
       lon_work,
       lat_work,
       cluster_work,
       cnt_work,
       max_distance,
       min_distance,
       confidence,
       substr(coalesce(geo6.province, geohash8_mapping.province_code, ''), 1, 2) as country,
       coalesce(geo6.province, geohash8_mapping.province_code, '') as province,
       coalesce(geo6.city,geohash8_mapping.city_code,'') as city,
       coalesce(geo6.area, geohash8_mapping.area_code, '') as area
    from
    (
    select
       device,
       centerlon AS lon_work,
       centerlat AS lat_work,
       cluster AS cluster_work,
       work_num1 as cnt_work,
       distance_max as max_distance,
       distance_min as min_distance,
       confidence,
       geohash6_mapping.province_code as province,
       geohash6_mapping.area_code as area,
       geohash6_mapping.city_code as city,
       geohash6_mapping.geohash_6_code
    from
    (
      select *,row_number() over(partition by device order by confidence desc) as rk
      from $tmp_device_work_place
      where stage in('A','B','C','D')
    ) work
    left join (select * from $geohash6_area_mapping_par where version='1000') geohash6_mapping
    on (get_geohash(centerlat, centerlon, 6) = geohash6_mapping.geohash_6_code)
    where work.rk = 1
    )geo6
    left join (select * from $geohash8_lbs_info_mapping_par where version='1000') geohash8_mapping
    on (case when geo6.geohash_6_code is null then get_geohash(lat_work, lon_work, 8) else concat('', rand()) end = geohash8_mapping.geohash_8_code)
  )t
  left  join
  (select country,country_code from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par') group by country,country_code) mapping_country
  on t.country=mapping_country.country_code
  left join
  (select province,province_code from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par') group by province,province_code) mapping_province
  on t.province=mapping_province.province_code
  left join
  (select city,city_code from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par') group by city,city_code) mapping_city
  on t.city=mapping_city.city_code
  left join
  (
   select area_poi,area_code from(
    select area_poi,area_code,row_number() over(partition by area_code order by length(area_poi)) as rank
    from $mapping_area_par where flag=GET_LAST_PARTITION('dm_sdk_mapping','mapping_area_par')
    ) min_area
    where rank=1
    ) mapping_area
  on t.area=mapping_area.area_code
)
insert overwrite table $rp_device_location_3monthly partition(day='${days}')
select
   device,
   round(lon_home,6) as lon_home,
   round(lat_home,6) as lat_home,
   cluster_home,
   cnt_home,
   max_distance_home,
   min_distance_home,
   round(confidence_home,4) as confidence_home,
   round(lon_work,6) as lon_work,
   round(lat_work,6) as lat_work,
   cluster_work,
   cnt_work,
   max_distance_work,
   min_distance_work,
   round(confidence_work,4) as confidence_work,
   case
     when lon_home is null or lat_home is null then 1 --无工作地
     when lon_work is null or lon_work is null then 2 --无居住地
     when get_distance(lon_home,lat_home,lon_work,lat_work)>100000 then 3 --距离过远
     when get_distance(lon_home,lat_home,lon_work,lat_work)<200 then 4 --距离过近
     else 5 --正常
   end as type,
   country_home,
   province_home,
   area_home,
   country_cn_home,
   province_cn_home,
   area_cn_home,
   country_work,
   province_work,
   area_work,
   country_cn_work,
   province_cn_work,
   area_cn_work,
   city_home,
   city_cn_home,
   city_work,
   city_cn_work
from
(
  select
     nvl(live_place.device,work_place.device) as device,
     live_place.lon_home,
     live_place.lat_home,
     live_place.cluster_home,
     live_place.cnt_home,
     live_place.max_distance as max_distance_home,
     live_place.min_distance as min_distance_home,
     live_place.confidence as confidence_home,
     live_place.country as country_home,
     live_place.province as province_home,
     live_place.city as city_home,
     live_place.area as area_home,
     live_place.country_cn as country_cn_home,
     live_place.province_cn as province_cn_home,
     live_place.city_cn as city_cn_home,
     live_place.area_cn as area_cn_home,
     work_place.lon_work,
     work_place.lat_work,
     work_place.cluster_work,
     work_place.cnt_work,
     work_place.max_distance as max_distance_work,
     work_place.min_distance as min_distance_work,
     work_place.confidence as confidence_work,
     work_place.country as country_work,
     work_place.province as province_work,
     work_place.city as city_work,
     work_place.area as area_work,
     work_place.country_cn as country_cn_work,
     work_place.province_cn as province_cn_work,
     work_place.city_cn as city_cn_work,
     work_place.area_cn as area_cn_work
  FROM
  live_place
  full join
  work_place
  on live_place.device = work_place.device
)t
"