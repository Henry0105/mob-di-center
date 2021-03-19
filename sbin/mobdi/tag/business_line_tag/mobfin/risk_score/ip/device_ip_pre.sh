#!/bin/bash
: '
@owner:luost
@describe:ip相关标签预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p30day=`date -d "$day -30 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#源表
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

#输出表
tmp_anticheat_device_ip_pre=dw_mobdi_tmp.tmp_anticheat_device_ip_pre

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_ip_pre partition(day = '$day')
select device,ipaddr,day as connect_day
from $dws_device_ip_info_di
where day > '$p30day' 
and day <= '$day' 
and ipaddr <> '' 
and ipaddr is not null;
"