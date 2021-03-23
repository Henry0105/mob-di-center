#!/bin/bash
: '
@owner:luost
@describe:反欺诈bssid连接量评分预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#入参
day=$1
p30day=`date -d "$day -30 days" +%Y%m%d`

#源表
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di

#输出表
tmp_anticheat_device_bssid_pre=dw_mobdi_tmp.tmp_anticheat_device_bssid_pre

hive -v -e "
create table if not exists $tmp_anticheat_device_bssid_pre(
    device string comment '设备号',
    bssid string comment 'bssid',
    connect_day string comment '连接日期',
    real_date string comment 'real_date'
)
comment '反欺诈bssid连接量评分预处理中间表'
partitioned by (day string comment '日期')
stored as orc;
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

insert overwrite table $tmp_anticheat_device_bssid_pre partition(day = '$day')
select device,bssid,day as connect_day,real_date
from 
(
    select muid as device,
           regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') as bssid,
           day,
           from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
    from $dwd_log_wifi_info_sec_di
    where day > '$p30day' 
    and day <= '$day'
    and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$' 
    and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') 
    and bssid is not null 
) as log_wifi_info 
where real_date > '$p30day'
and real_date <= '$day';
"
