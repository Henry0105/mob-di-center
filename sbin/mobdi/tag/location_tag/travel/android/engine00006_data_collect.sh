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
#mapping_city_level_par=dm_sdk_mapping.mapping_city_level_par

## 目标表
engine00006_data_collect=dm_mobdi_tmp.engine00006_data_collect



HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/dependencies/lib/lamfire-2.1.4.jar;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function coordConvert as 'com.youzu.mob.java.udf.CoordConvertor';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';


insert overwrite table $engine00006_data_collect partition(day='$day')
select device,day,
concat_ws(',',collect_list(city)) as citylist,
concat_ws(',',collect_list(level)) as levellist
from(
select device, day, if(city like 'cn%',city,split(city,'[0-9]')[0]) as city, nvl(level,if(city not like 'cn%','0',null)) as level
from(
select
t1.device,t1.city,day,level
from
(
 select device , city , days as day
        from(
              select *
              from
                 $tmp_engine00002_datapre
              where
                  day='$day'
            ) tt
            where city     <> city_home
            and city <> city_work
            and (length(city_home)    > 0 or length(city_work) > 0)
            and length(trim(city))>0
)t1
left join
(
select city_code, level
from $mapping_city_level_par
where version='1001'
)t2
on t1.city = t2.city_code
)tt
where length(nvl(level,if(city not like 'cn%','0',null)))>0
group by device,day,city,level
) ttt
group by device,day;
"
