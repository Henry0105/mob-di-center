#!/bin/bash
: '
@owner:luost
@describe:gps,ip城市不一致次数
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
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_device_gps_ip_location=dw_mobdi_tmp.tmp_anticheat_device_gps_ip_location

#输出表
#label_l1_anticheat_device_cnt=dm_mobdi_report.label_l1_anticheat_device_cnt

function device_gps_ip(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
SET hive.exec.parallel=true;
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

insert overwrite table $label_l1_anticheat_device_cnt partition(day = '$day',timewindow = '$timewindow',flag = '2')
select device,count(1) as cnt
from $tmp_anticheat_device_gps_ip_location
where day = '$day'
and timewindow = '$timewindow'
and city_code_ip <> city_code_gps
group by device;
" 
}

for i in 7 14 30
do
    device_gps_ip $i
done