#!/bin/bash
: '
@owner:luost
@describe:bssid连接量评分标签
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

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore

function bssidcnt_riskScore(){

#源表
table=$dw_mobdi_tmp.tmp_anticheat_device_bssid_cnt_$1days
timewindow=$1

q1=`hive -e "select percentile(cnt,0.25) from $table where day = '$day';"`
q3=`hive -e "select percentile(cnt,0.75) from $table where day = '$day';"`
maxValue=`hive -e "select percentile(cnt,0.99) from $table where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition(day = '$day',timewindow = '$timewindow',flag = '6')
select device,
case 
  when cnt <= ($q3+1.5*($q3-$q1)) then 0
  when cnt > ($q3+1.5*($q3-$q1)) and cnt <= $maxValue then cnt*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
  when cnt > $maxValue then 1
  end as riskScore
from $table
where day = '$day';
"
}

for i in 7 14 30
do
    bssidcnt_riskScore $i
done