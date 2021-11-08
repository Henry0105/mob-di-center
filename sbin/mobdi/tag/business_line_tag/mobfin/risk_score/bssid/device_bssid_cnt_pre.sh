#!/bin/bash
: '
@owner:luost
@describe:bssid连接量评分标签预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

tmpdb=$dw_mobdi_tmp
#源表
tmp_anticheat_device_bssid_pre=$tmpdb.tmp_anticheat_device_bssid_pre

#输出表
tmp_anticheat_device_bssid_cnt_30days=${tmpdb}.tmp_anticheat_device_bssid_cnt_30days

function bssidcnt(){

table=dw_mobdi_md.tmp_anticheat_device_bssid_cnt_$1days
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $table partition(day = '$day')
select device,count(1) as cnt
from 
(
    select device,bssid
    from $tmp_anticheat_device_bssid_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$pday'
    and real_date <= '$day'
    and real_date > '$pday'
    group by device,bssid
) a
group by device;
"
}

for i in 7 14 30
do
    bssidcnt $i
done