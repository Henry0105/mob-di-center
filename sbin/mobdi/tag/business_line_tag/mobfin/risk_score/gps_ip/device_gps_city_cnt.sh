#!/bin/bash
: '
@owner:luost
@describe:近一个月GPS城市变化次数
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

#源表
tmp_anticheat_device_gps_ip_location=$dw_mobdi_tmp.tmp_anticheat_device_gps_ip_location

#输出表
#label_l1_anticheat_device_cnt=dm_mobdi_report.label_l1_anticheat_device_cnt

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

insert overwrite table $label_l1_anticheat_device_cnt partition(day = '$day',timewindow = '30',flag = '1')
select device,count(1) as cnt
from 
(
    select device
    from $tmp_anticheat_device_gps_ip_location
    where day = '$day'
    and timewindow = '30'
    group by device,city_code_gps
)a
group by device;
" 