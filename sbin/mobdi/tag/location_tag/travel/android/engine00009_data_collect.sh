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

source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

## 源表
tmp_engine00002_datapre=dm_mobdi_tmp.tmp_engine00002_datapre
#rp_device_location_permanent=rp_mobdi_app.rp_device_location_permanent


## mapping 表
#map_province_loc=dm_sdk_mapping.map_province_loc

## 目标表
engine00009_data_collect=dm_mobdi_tmp.engine00009_data_collect

sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_report', 'rp_device_location_permanent', 'day');
"
rp_device_location_permanent_lastday=(`hive -e "$sql"`)


sql_final="
insert overwrite table $engine00009_data_collect partition(day='$day')
select device, day, case when sum(around_flag) > 0 then 1 else 0 end as around_flag
from (
    select device, day, case when around_province is not null and travel_province is not null and travel_province = 
                around_province then 1 else 0 end around_flag
    from (
        select device, day, t1.travel_province, t2.province2_code as around_province
        from (
            select a1.device, a1.day, a1.province as travel_province, a2.province as base_province
            from (
                select device, days as day, province from 
                (select * from $tmp_engine00002_datapre where day='$day') tt
                where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0
                        and city is not null and length(trim(city))!=0 )
                ) a1
            left join (
                select device, province
                from $rp_device_location_permanent
                where day = '$rp_device_location_permanent_lastday'
                ) a2 on a1.device = a2.device
            ) t1
        left join (
            select province1_code, province2_code
            from $map_province_loc
            where flag = 1
            ) t2 on t1.base_province = t2.province1_code
        ) tt
    ) ttt
group by device, day;
"

hive_setting "$sql_final"

