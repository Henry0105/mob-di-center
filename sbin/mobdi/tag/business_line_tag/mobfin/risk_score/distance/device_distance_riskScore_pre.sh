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

#源表
tmp_anticheat_device_distance_pre=dw_mobdi_tmp.tmp_anticheat_device_distance_pre

#输出表
tmp_anticheat_device_avgdistance_pre=dw_mobdi_tmp.tmp_anticheat_device_avgdistance_pre
tmp_anticheat_device_nightdistance_pre=dw_mobdi_tmp.tmp_anticheat_device_nightdistance_pre
tmp_anticheat_device_alldistance_pre=dw_mobdi_tmp.tmp_anticheat_device_alldistance_pre

hive -v -e "
create table if not exists $tmp_anticheat_device_avgdistance_pre(
    device string comment '设备号',
    distance bigint comment '距离'
)
comment '移动平均距离预处理中间表'
partitioned by (
    day string comment '日期',
    timewindow string comment '时间窗:7天，14天，30天')
stored as orc;

create table if not exists $tmp_anticheat_device_nightdistance_pre(
    device string comment '设备号',
    distance bigint comment '距离'
)
comment '夜晚移动距离预处理中间表'
partitioned by (
    day string comment '日期',
    timewindow string comment '时间窗:7天，14天，30天')
stored as orc;

create table if not exists $tmp_anticheat_device_alldistance_pre(
    device string comment '设备号',
    distance bigint comment '距离'
)
comment '总移动距离预处理中间表'
partitioned by (
    day string comment '日期',
    timewindow string comment '时间窗:7天，14天，30天')
stored as orc;
"

function device_avgdistance(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

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

insert overwrite table $tmp_anticheat_device_avgdistance_pre partition (day = '$day',timewindow = '$timewindow')
select device,avg(distance_byday) as distance
from 
(
    select device,connect_day,sum(distance) as distance_byday
    from $tmp_anticheat_device_distance_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$pday'
    group by device,connect_day
) as a 
group by device;
"

}

function device_nightdistance(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

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

insert overwrite table $tmp_anticheat_device_nightdistance_pre partition (day = '$day',timewindow = '$timewindow')
select device,avg(distance_byday) as distance 
from 
(
    select device,connect_day,sum(distance) as distance_byday
    from $tmp_anticheat_device_distance_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$pday'
    and (hour(start_time) >= 22 and hour(start_time) <= 23) 
    or (hour(start_time) >= 0 and hour(start_time) < 6)
    group by device,connect_day
) as a 
group by device;
"
}

function device_alldistance(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

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

insert overwrite table $tmp_anticheat_device_alldistance_pre partition (day = '$day',timewindow = '$timewindow')
select device,sum(distance) as distance
from $tmp_anticheat_device_distance_pre
where day = '$day'
and connect_day <= '$day'
and connect_day > '$pday'
group by device;
"  
}


for i in 7 14 30
do
    device_avgdistance $i

    device_nightdistance $i

    device_alldistance $i
done