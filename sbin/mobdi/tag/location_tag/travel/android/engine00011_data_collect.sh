#!/bin/bash

source ../../../../util/util.sh

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi


day=$1

source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

## 源表
tmp_engine00002_datapre=dm_mobdi_tmp.tmp_engine00002_datapre

## mapping 表
#poi_config_mapping_par=dm_sdk_mapping.poi_config_mapping_par
#dim_hotel_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_hotel_ssid_bssid_match_info_mf

## 目标表
engine00011_data_collect=dm_mobdi_tmp.engine00011_data_collect

ip_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_sdk_mapping', 'dim_hotel_ssid_bssid_match_info_mf', 'day');
"
hotel_ssid_bssid=(`hive -e "$ip_mapping_sql"`)


sql_final="

with data_pre as(
select a1.device, a1.city, a2.hotel_style, a2.rank_star, a2.score_type, a2.price_level, a2.brand
from
(select device,days as day,orig_note1,city
    from
    (select * from $tmp_engine00002_datapre where day='$day') tt
    where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
    and city is not null and length(trim(city))>=0
)a1
left join
(

select bssid,name,poi_lat,poi_lon,rank_star,score_type,price_level,hotel_style,brand
from(
select t1.bssid_array,t1.name,t1.poi_lat,t1.poi_lon,
       t2.rank_star,t2.score_type,t2.price_level,t2.hotel_style,t2.brand
from
(
select * from
$dim_hotel_ssid_bssid_match_info_mf
where day='${hotel_ssid_bssid}'
)t1
left join
(
  select name,poi_lat,poi_lon,rank_star,score_type,price_level,hotel_style,brand
  from
  (
    select trim(name) as name,
           lat as poi_lat,
           lon as poi_lon,
           get_json_object(attribute,'$.rank_star') as rank_star,
           get_json_object(attribute,'$.score_type') as score_type,
           get_json_object(attribute,'$.price_level') as price_level,
           get_json_object(attribute,'$.hotel_style') as hotel_style,
           get_json_object(attribute,'$.brand') as brand
    from $poi_config_mapping_par
    where type=6
    and version='1000'
  ) t
  group by name,poi_lat,poi_lon,rank_star,score_type,price_level,hotel_style,brand
) t2
on t1.name=t2.name and t1.poi_lat=t2.poi_lat and t1.poi_lon=t2.poi_lon
) tt
lateral view explode(bssid_array) t as bssid
) a2
on regexp_replace(a1.orig_note1,'bssid=','')=a2.bssid
where a2.hotel_style is not null or  a2.rank_star is not null or
      a2.score_type is not null or a2.price_level is not null or  a2.brand is not null and length(a2.bssid)>0
group by
a1.device,a1.city, a2.hotel_style, a2.rank_star, a2.score_type, a2.price_level, a2.brand
)
insert overwrite table $engine00011_data_collect partition(day='$day')
select t1.device,t1.city
      ,t2.hotel_style
      ,t3.rank_star
      ,t4.score_type
      ,t5.price_level
      ,t6.brand
from
(select device,city from data_pre group by device,city) t1
left join
(select device,city,collect_set(map(hotel_style,hotel_style_num)) hotel_style
from
(select device,city,hotel_style,count(1) hotel_style_num
from data_pre
group by device,city,hotel_style
)a group by device,city
)t2
on t1.device=t2.device and t1.city=t2.city

left join
(select device,city,collect_set(map(rank_star,rank_star_num)) rank_star
from
(select device,city,rank_star,count(1) rank_star_num
from data_pre
group by device,city,rank_star
)a group by device,city
)t3
on  t1.device=t3.device and t1.city=t3.city

left join
(select device,city,collect_set(map(score_type,score_type_num)) score_type
from
(select device,city,score_type,count(1) score_type_num
from data_pre
group by device,city,score_type
)a group by device,city
)t4
on  t1.device=t4.device and t1.city=t4.city

left join
(select device,city,collect_set(map(price_level,price_level_num)) price_level
from
(select device,city,price_level,count(1) price_level_num
from data_pre
group by device,city,price_level
)a group by device,city
)t5
on  t1.device=t5.device and t1.city=t5.city

left join
(select device,city,collect_set(brand)  brand
from data_pre
group by device,city
)t6
on  t1.device=t6.device and t1.city=t6.city;
"

hive_setting "$sql_final"