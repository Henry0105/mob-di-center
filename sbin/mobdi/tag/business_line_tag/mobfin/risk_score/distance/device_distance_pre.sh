#!/bin/bash
: '
@owner:luost
@describe:移动距离评分预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1
p30day=`date -d "$day -30 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di

#输出表
tmp_anticheat_device_distance_pre=$dw_mobdi_tmp.tmp_anticheat_device_distance_pre

hive -v -e "
create table if not exists $tmp_anticheat_device_distance_pre(
    device string comment '设备号',
    start_time string comment '开始时间',
    distance double comment '移动距离',
    connect_day string comment '日期'
)
comment '移动距离评分预处理中间表'
partitioned by (day string comment '日期')
stored as orc;
"

hive -v -e "
SET hive.exec.parallel=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

with distance_pre as 
(
    select device,lat,lon,start_time,day,
           type,orig_note1,get_geohash(lat, lon, 5) as geohash5
    from $dws_device_location_staying_di
    where day > '$p30day' 
    and day <= '$day' 
    and type in ('gps', 'wifi') 
    and data_source not in ('unknown_gps', 'unknown_auto')
),

strange_wifi as(
    select orig_note1
    from 
    (
        select orig_note1,geohash5,count(1) as cnt
        from distance_pre
        where type = 'wifi' 
        and orig_note1 like 'bssid=%'
        group by orig_note1,geohash5
    ) as m 
    group by orig_note1
    having count(*) > 1
),

union_distance_info as(
    select device,lat,lon,start_time,day
    from 
    (
        select device,lat,lon,start_time,day
        from 
        (
            select device,lat,lon,orig_note1,start_time,day
            from distance_pre
            where type = 'wifi'
        ) as a 
        left join strange_wifi
        on a.orig_note1 = strange_wifi.orig_note1
        where strange_wifi.orig_note1 is null

        union all 

        select device,lat,lon,start_time,day
        from distance_pre
        where type <> 'wifi'
    ) as m 
    distribute by device sort by device,day,start_time
)

insert overwrite table $tmp_anticheat_device_distance_pre partition(day = '$day')
select device,start_time,get_distance(lat, lon, lat1, lon1) as distance,day as connect_day
from 
(
  select device,lat,lon,
         lead(lat, 1) over(partition by device order by day, start_time) as lat1, 
         lead(lon, 1) over(partition by device order by day, start_time) as lon1, 
         start_time,day
  from union_distance_info
) as a 
where lat1 is not null and lon1 is not null;
"
