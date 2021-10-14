#!/bin/bash

set -e -x

: '
@owner:zhangjch
@describe:商场报点指标监控
@projectName:mob_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

province_cn="'cn0', 'cn1', 'cn2', 'cn4', 'cn9', 'cn3', 'cn13', 'cn18', 'cn6', 'cn7', 'cn5', 'cn12', 'cn8', 'cn16', 'cn15', 'cn10', 'cn29', 'cn17', 'cn25', 'cn20', 'cn22', 'cn11', 'cn23', 'cn14', 'cn19'"

city="'cn0_01', 'cn1_01', 'cn2_04', 'cn2_09', 'cn4_07', 'cn9_18', 'cn3_13', 'cn3_04', 'cn13_03', 'cn18_06', 'cn6_01', 'cn4_06', 'cn7_01', 'cn5_02', 'cn12_10', 'cn5_16', 'cn8_15', 'cn2_23', 'cn16_02', 'cn3_10', 'cn15_06', 'cn10_16', 'cn29_10', 'cn16_07', 'cn15_02', 'cn2_13', 'cn17_05', 'cn25_04', 'cn20_02', 'cn2_06', 'cn3_12', 'cn22_09', 'cn4_01', 'cn3_11', 'cn5_12', 'cn15_09', 'cn11_11', 'cn23_02', 'cn14_12', 'cn19_10'"


day=$1

source /home/dba/mobdi_center/conf/hive-env.sh


#input
#dwd_device_location_di_v2=dm_mobdi_master.dwd_device_location_di_v2
#mobeye_mall_polygon_final=dm_mobeye_master.mobeye_mall_polygon_final

#md
#mall_polygon_device_active_di=mob_dashboard.mall_polygon_device_active_di
#mall_polygon_click_dau_di=mob_dashboard.mall_polygon_click_dau_di

HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_mobdashboard.mobdashboard;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash_adjacent as 'com.youzu.mob.java.udf.GeohashAdjacent';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';

--每日活跃device  500米近场调频 明细
insert overwrite table $mall_polygon_device_active_di partition(day='$day')
select a.device
     , a.province_cn
	   , b.mob_mall_id
	   , b.mob_mall_name
	   , b.city_cn
	   , b.area_cn
	   , case when a.city in ('cn0_01', 'cn1_01', 'cn2_04', 'cn2_09', 'cn4_07' )
	            then '北上广深杭'
		        else '其他'
       end as city_type
from (
    select *
	  from $dwd_device_location_di_v2
    where day = '$day'
      and province_cn in ($province_cn)
      and city in ($city)
) as a
inner join (
    select *
    from $mobeye_mall_polygon_final
         lateral view explode(split(get_geohash_adjacent(get_geohash(lat,lon,5)),','))t as geohash5
) as b
on get_geohash(a.lat,a.lon,5) = b.geohash5
where get_distance(b.lat,b.lon,a.lat,a.lon)<=500
;

--每日商场附近的点击和日活统计
insert overwrite table $mall_polygon_click_dau_di partition(day='$day')
select city_type,
       province_cn,
       city_cn,
       area_cn,
       mob_mall_name,
       sum(cnt) as click_cnt,
       count(*) as dau
from (
    select city_type,
           province_cn,
           city_cn,
           area_cn,
           device,
           concat_ws('-',mob_mall_name,mob_mall_id) as mob_mall_name,
           count(*) cnt
    from $mall_polygon_device_active_di
    where day = '$day'
    group by city_type,
             province_cn,
             city_cn,
             area_cn,
             device,
             mob_mall_id,
             mob_mall_name
) a
group by city_type,
         province_cn,
         city_cn,
         area_cn,
         mob_mall_name
;
"









































