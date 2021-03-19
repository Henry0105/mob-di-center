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
p7day=`date -d "$day -7 days" +%Y%m%d`
p14day=`date -d "$day -14 days" +%Y%m%d`
p30day=`date -d "$day -30 days" +%Y%m%d`

#源表
tmp_anticheat_device_ip_pre=dw_mobdi_tmp.tmp_anticheat_device_ip_pre

#输出表
tmp_anticheat_device_ip_cnt_7days=dw_mobdi_tmp.tmp_anticheat_device_ip_cnt_7days
tmp_anticheat_device_ip_cnt_14days=dw_mobdi_tmp.tmp_anticheat_device_ip_cnt_14days
tmp_anticheat_device_ip_cnt_30days=dw_mobdi_tmp.tmp_anticheat_device_ip_cnt_30days
tmp_anticheat_device_first3_ip_cnt_7days=dw_mobdi_tmp.tmp_anticheat_device_first3_ip_cnt_7days
tmp_anticheat_device_first3_ip_cnt_14days=dw_mobdi_tmp.tmp_anticheat_device_first3_ip_cnt_14days
tmp_anticheat_device_first3_ip_cnt_30days=dw_mobdi_tmp.tmp_anticheat_device_first3_ip_cnt_30days

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_ip_cnt_7days partition (day = '$day')
select device,count(1) as cnt
from 
(
    select device,ipaddr
    from $tmp_anticheat_device_ip_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$p7day'
    group by device,ipaddr
) a
group by device;

insert overwrite table $tmp_anticheat_device_ip_cnt_14days partition (day = '$day')
select device,count(1) as cnt
from 
(
    select device,ipaddr
    from $tmp_anticheat_device_ip_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$p14day'
    group by device,ipaddr
) a
group by device;

insert overwrite table $tmp_anticheat_device_ip_cnt_30days partition (day = '$day')
select device,count(1) as cnt
from 
(
    select device,ipaddr
    from $tmp_anticheat_device_ip_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$p30day'
    group by device,ipaddr
) a
group by device;
"

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $tmp_anticheat_device_first3_ip_cnt_7days partition (day = '$day')
select device, count(1) as cnt
from 
(
    select device
    from $tmp_anticheat_device_ip_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$p7day'
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
) a 
group by device;

insert overwrite table $tmp_anticheat_device_first3_ip_cnt_14days partition (day = '$day')
select device, count(1) as cnt
from 
(
    select device
    from $tmp_anticheat_device_ip_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$p14day'
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
) a 
group by device;

insert overwrite table $tmp_anticheat_device_first3_ip_cnt_30days partition (day = '$day')
select device, count(1) as cnt
from 
(
    select device
    from $tmp_anticheat_device_ip_pre
    where day = '$day'
    and connect_day <= '$day'
    and connect_day > '$p30day'
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
) a 
group by device;
"