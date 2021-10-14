#!/bin/bash
: '
@owner:luost
@describe:近90天移动距离评分
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p90day=`date -d "$day -90 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di
tmpdb=$dw_mobdi_tmp
#tmp
tmp_anticheat_device_distance_90days_pre=$tmpdb.tmp_anticheat_device_distance_90days_pre
tmp_anticheat_device_avgdistance_pre=$tmpdb.tmp_anticheat_device_avgdistance_pre
tmp_anticheat_device_nightdistance_pre=$tmpdb.tmp_anticheat_device_nightdistance_pre
tmp_anticheat_device_alldistance_pre=$tmpdb.tmp_anticheat_device_alldistance_pre

#输出表
#label_l1_anticheat_device_riskscore=dm_mobdi_report.label_l1_anticheat_device_riskscore

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

with distance_pre as 
(
    select device,lat,lon,start_time,day,
           type,orig_note1,get_geohash(lat, lon, 5) as geohash5
    from $dws_device_location_staying_di
    where day > '$p90day' 
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

insert overwrite table $tmp_anticheat_device_distance_90days_pre partition(day = '$day')
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

#每日平均移动距离
hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_avgdistance_pre partition (day = '$day',timewindow = '90')
select device,avg(distance_byday) as distance
from 
(
    select device,connect_day,sum(distance) as distance_byday
    from $tmp_anticheat_device_distance_90days_pre
    where day = '$day'
    group by device,connect_day
) as a 
group by device;
"

#每日夜晚移动平均距离
hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_nightdistance_pre partition (day = '$day',timewindow = '90')
select device,avg(distance_byday) as distance 
from 
(
    select device,connect_day,sum(distance) as distance_byday
    from $tmp_anticheat_device_distance_90days_pre
    where day = '$day'
    and (hour(start_time) >= 22 and hour(start_time) <= 23) 
    or (hour(start_time) >= 0 and hour(start_time) < 6)
    group by device,connect_day
) as a 
group by device;
"

#移动总距离
hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_alldistance_pre partition (day = '$day',timewindow = '90')
select device,sum(distance) as distance
from $tmp_anticheat_device_distance_90days_pre
where day = '$day'
group by device;
"

avgq1=`hive -e "select percentile(distance,0.25) from $tmp_anticheat_device_avgdistance_pre where day = '$day' and timewindow = '90'"`
avgq3=`hive -e "select percentile(distance,0.75) from $tmp_anticheat_device_avgdistance_pre where day = '$day' and timewindow = '90'"`
avgMaxValue=`hive -e "select percentile(distance,0.99) from $tmp_anticheat_device_avgdistance_pre where day = '$day' and timewindow = '90'"`

nightq1=`hive -e "select percentile(distance,0.25) from $tmp_anticheat_device_nightdistance_pre where day = '$day' and timewindow = '90'"`
nightq3=`hive -e "select percentile(distance,0.75) from $tmp_anticheat_device_nightdistance_pre where day = '$day' and timewindow = '90'"`
nightMaxValue=`hive -e "select percentile(distance,0.99) from $tmp_anticheat_device_nightdistance_pre where day = '$day' and timewindow = '90'"`

allq1=`hive -e "select percentile(distance,0.25) from $tmp_anticheat_device_alldistance_pre where day = '$day' and timewindow = '90'"`
allq3=`hive -e "select percentile(distance,0.75) from $tmp_anticheat_device_alldistance_pre where day = '$day' and timewindow = '90'"`
allMaxValue=`hive -e "select percentile(distance,0.99) from $tmp_anticheat_device_alldistance_pre where day = '$day' and timewindow = '90'"`

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskscore partition (day = '$day' ,timewindow = '90',flag = '8')
select device,avg(riskScore) as riskScore
from 
(
    select device,
    case
      when distance = 0 then 1
      when distance > 0 and distance <= 100 then 0.8
      when distance > 100 and distance <= ($avgq3+1.5*($avgq3-$avgq1)) then 0
      when distance > ($avgq3+1.5*($avgq3-$avgq1)) and distance <= $avgMaxValue 
        then distance*1/($avgMaxValue-($avgq3+1.5*($avgq3-$avgq1))) + (1-$avgMaxValue*1/($avgMaxValue-($avgq3+1.5*($avgq3-$avgq1))))
      when distance > $avgMaxValue then 1
      end as riskScore
    from $tmp_anticheat_device_avgdistance_pre
    where day = '$day' 
    and timewindow = '90'

    union all 

    select device,
    case
      when distance <= ($nightq3+1.5*($nightq3-$nightq1)) then 0
      when distance > ($nightq3+1.5*($nightq3-$nightq1)) and distance <= $nightMaxValue 
        then distance*1/($nightMaxValue-($nightq3+1.5*($nightq3-$nightq1))) + (1-$nightMaxValue*1/($nightMaxValue-($nightq3+1.5*($nightq3-$nightq1)))) 
      when distance > $nightMaxValue then 1
      end as riskScore
    from $tmp_anticheat_device_nightdistance_pre
    where day = '$day' 
    and timewindow = '90'

    union all

    select device,
    case
      when distance = 0 then 1
      when distance > 0 and distance <= 100 then 0.8
      when distance > 100 and distance <= ($allq3+1.5*($allq3-$allq1)) then 0
      when distance > ($allq3+1.5*($allq3-$allq1)) and distance <= $allMaxValue 
        then distance*1/($allMaxValue-($allq3+1.5*($allq3-$allq1))) + (1-$allMaxValue*1/($allMaxValue-($allq3+1.5*($allq3-$allq1))))
      when distance > $allMaxValue then 1
      end as riskScore
    from $tmp_anticheat_device_alldistance_pre
    where day = '$day' 
    and timewindow = '90'
)a
group by device;
"