#!/bin/bash

source ../../../../util/util.sh

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi


day=$1
insert_day=${day:0:6}01

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dm_mobdi_tmp

## 源表
tmp_engine00002_datapre=$tmpdb.tmp_engine00002_datapre

## mapping 表

#dim_traffic_ssid_bssid_match_info_mf=dim_mobdi_mapping.dim_traffic_ssid_bssid_match_info_mf
#dim_traffic_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_traffic_ssid_bssid_match_info_mf

## 目标表
engine00005_data_collect=$tmpdb.engine00005_data_collect


mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_traffic_ssid_bssid_match_info_mf', 'day');
"
last_mapping_partition=(`hive -e "$mapping_sql"`)


hive -v -e "
insert overwrite table $engine00005_data_collect partition(day='$insert_day')
select device,day,traffic_list
from (
select device,day,collect_set(type_name) as traffic_list
from(
select t1.device, t1.day,t2.type_name
from
(select device,days as day ,city,orig_note1
from
(select * from $tmp_engine00002_datapre where day='$insert_day') tt
where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
and length(city)>0
) t1
left join
(
select type_name,bssid
from
(select *
from
$dim_traffic_ssid_bssid_match_info_mf
where day='$last_mapping_partition' and type_name<>'地铁站') aa
lateral view explode(bssid_array) t as bssid
)t2
on regexp_replace(t1.orig_note1,'bssid=','')=t2.bssid
)tt
group by device,day
) aa
where size(traffic_list)>0;
"
