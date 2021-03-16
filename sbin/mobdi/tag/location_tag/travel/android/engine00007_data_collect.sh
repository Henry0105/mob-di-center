#!/bin/bash

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi


day=$1

source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

## 源表
tmp_engine00002_datapre=dm_mobdi_tmp.tmp_engine00002_datapre

## mapping 表
#mapping_area_par=dm_sdk_mapping.mapping_area_par

## 目标表
engine00007_data_collect=dm_mobdi_tmp.engine00007_data_collect


ip_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_sdk_mapping', 'mapping_area_par', 'flag');
"
mapping_area_par_lastday=(`hive -e "$ip_mapping_sql"`)


hive -v -e "
insert overwrite table $engine00007_data_collect partition(day='$day')
select device,coalesce(continents,if(t1.city like '%cn',null,t1.city)) as continents,
coalesce(country_code,if(t1.city like '%cn',null,t1.city)) as country_code,coalesce(country,if(t1.city like '%cn',null,t1.city)) as country_poi,
coalesce(province_code,if(t1.city like '%cn',null,t1.city)) as province_code,coalesce(province,if(t1.city like '%cn',null,t1.city)) as province_poi,
coalesce(city_code,if(t1.city like '%cn',null,t1.city)) as city_code,coalesce(t2.city,if(t1.city like '%cn',null,t1.city)) as city_poi
from
(select device,city
from
(select * from $tmp_engine00002_datapre where day='$day') tt
where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0
) and city is not null and length(trim(city))>0
group by device,city)t1
left join
(
select continents,country,province,city,area,country_code,province_code,city_code,area_code
from
$mapping_area_par
where flag='20191010' and length(trim(city_code))>0 and city_code is not null
group by continents,country,province,city,area,country_code,province_code,city_code,area_code
)t2
on t1.city = t2.city_code
;
"
