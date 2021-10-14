#!/bin/bash
: '
@owner:luost
@describe:移动距离评分标签
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

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dw_mobdi_tmp
#源表
tmp_anticheat_device_avgdistance_pre=$tmpdb.tmp_anticheat_device_avgdistance_pre
tmp_anticheat_device_nightdistance_pre=$tmpdb.tmp_anticheat_device_nightdistance_pre
tmp_anticheat_device_alldistance_pre=$tmpdb.tmp_anticheat_device_alldistance_pre

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore

function device_distance(){

timewindow=$1

avgq1=`hive -e "select percentile(distance,0.25) from $tmp_anticheat_device_avgdistance_pre where day = '$day' and timewindow = '$timewindow'"`
avgq3=`hive -e "select percentile(distance,0.75) from $tmp_anticheat_device_avgdistance_pre where day = '$day' and timewindow = '$timewindow'"`
avgMaxValue=`hive -e "select percentile(distance,0.99) from $tmp_anticheat_device_avgdistance_pre where day = '$day' and timewindow = '$timewindow'"`

nightq1=`hive -e "select percentile(distance,0.25) from $tmp_anticheat_device_nightdistance_pre where day = '$day' and timewindow = '$timewindow'"`
nightq3=`hive -e "select percentile(distance,0.75) from $tmp_anticheat_device_nightdistance_pre where day = '$day' and timewindow = '$timewindow'"`
nightMaxValue=`hive -e "select percentile(distance,0.99) from $tmp_anticheat_device_nightdistance_pre where day = '$day' and timewindow = '$timewindow'"`

allq1=`hive -e "select percentile(distance,0.25) from $tmp_anticheat_device_alldistance_pre where day = '$day' and timewindow = '$timewindow'"`
allq3=`hive -e "select percentile(distance,0.75) from $tmp_anticheat_device_alldistance_pre where day = '$day' and timewindow = '$timewindow'"`
allMaxValue=`hive -e "select percentile(distance,0.99) from $tmp_anticheat_device_alldistance_pre where day = '$day' and timewindow = '$timewindow'"`

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

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day' ,timewindow = '$timewindow',flag = '8')
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
    and timewindow = '$timewindow'

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
    and timewindow = '$timewindow'

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
    and timewindow = '$timewindow'
)a
group by device;
"
}


for i in 7 14 30
do
    device_distance $i
done