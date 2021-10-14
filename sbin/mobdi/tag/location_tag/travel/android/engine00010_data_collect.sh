#!/bin/bash

source ../../../../util/util.sh

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi


day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dm_mobdi_tmp

## 源表
tmp_engine00002_datapre=$tmpdb.tmp_engine00002_datapre

## mapping 表
#dim_map_province_loc=dim_sdk_mapping.dim_map_province_loc
#map_province_loc=dm_sdk_mapping.map_province_loc

## 目标表
engine00010_data_collect=$tmpdb.engine00010_data_collect


sql_final="
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

insert overwrite table $engine00010_data_collect partition(day='$day')
select device,days,max(travel_type) as travel_type
from(
select device,day as days,city,t1.travel_province,t1.base_province,t2.province1_code,t2.province2_code,
     case when t2.province2_code is null and t1.travel_province not like 'cn%' then 3
          when (t2.province2_code is not null or t1.base_province=t1.travel_province) then 1
          else 2 end as travel_type
     from(
            select a1.device,a1.day,country,city,
            a1.province as travel_province,
            base_province
            from
                (select device,days as day,country,province,split(city_work,'_')[0] as base_province,city
                 from (
                      select device,days,country,province,city_home,city,city_work from
                      (select device,days,country,province,city_home,city,city_work from $tmp_engine00002_datapre where day='$day') tt
                      where city <> city_home and city <> city_work and length(city_work) > 0 and length(city)>0
                      ) m1
                )a1
     )t1
     left join
         (
         select country, province1_code, province2_code
         from $dim_map_province_loc
         where flag='1'
         )t2
         on t1.base_province = t2.province1_code
         and t1.travel_province = t2.province2_code

union all

select device,day as days,city,t1.travel_province,t1.base_province,t2.province1_code,t2.province2_code,
     case when t2.province2_code is null and t1.travel_province not like 'cn%' then 3
          when (t2.province2_code is not null or t1.base_province=t1.travel_province) then 1
          else 2 end as travel_type
     from(
            select a1.device,a1.day,country,city,
            a1.province as travel_province,
            base_province
            from
                (select device,days as day,country,province,split(city_home,'_')[0] as base_province,city
                 from (
                      select device,days,country,province,city_home,city,city_work from
                      (select device,days,country,province,city_home,city,city_work from $tmp_engine00002_datapre where day='$day') tt
                      where city <> city_home and city <> city_work and length(city_home) > 0 and length(city)>0
                      ) m1
                )a1
     )t1
     left join
         (
         select country, province1_code, province2_code
         from $dim_map_province_loc
         where flag='1'
         )t2
         on t1.base_province = t2.province1_code
         and t1.travel_province = t2.province2_code
)tt
group by device,days;
"

hive_setting "$sql_final"