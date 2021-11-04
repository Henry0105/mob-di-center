#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:income_1001特征较多，本脚本做一次初步计算汇总
@projectName:mobdi
@BusinessName:profile_model
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

#input
device_applist_new=$dim_device_applist_new_di
log_wifi_info=$dwd_log_wifi_info_sec_di

#tmp
dws_location_bssid_info_di=$dws_location_bssid_info_di

#out
calculate_model_device=dm_mobdi_tmp.calculate_model_device
income_1001_bssid_index_calculate_base_info=dm_mobdi_tmp.income_1001_bssid_index_calculate_base_info

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.job.queuename=root.yarn_data_compliance2;

insert overwrite table $calculate_model_device partition(day='$day')
select device
from $device_applist_new
where day = '$day'
group by device;
"

:<<!
--先做一层dws聚合表，跑一个月数据
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table dm_mobdi_topic.dws_location_bssid_info_di partition(day)
select if(plat = 1,muid,device) as device,bssid,plat,
       from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss') as real_date, day
from dm_mobdi_master.dwd_log_wifi_info_sec_di
where day <= '20210920'
and day >= '20210820'
and trim(bssid) not in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
and trim(bssid) is not null
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
group by if(plat = 1,muid,device),bssid,plat,
         from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss'), day;
!

#每日跑一天的dws聚合表数据
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

set mapreduce.job.queuename=root.yarn_data_compliance2;

insert overwrite table $dws_location_bssid_info_di partition(day='$day')
select if(plat = 1,muid,device) as device,bssid,plat,
       from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss') as real_date
from $log_wifi_info
where day = '$day'
and trim(bssid) not in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
and trim(bssid) is not null
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
group by if(plat = 1,muid,device),bssid,plat,
         from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss');
"

p1month=`date -d "$day -1 months" +%Y%m%d`
#聚合一个月活跃的bssid
#与需要计算的设备数据inner join
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

set mapreduce.job.queuename=root.yarn_data_compliance2;

insert overwrite table $income_1001_bssid_index_calculate_base_info partition(day='$day')
select a.device,b.bssid,b.day as active_day
from $calculate_model_device a
inner join
(
  select device,bssid,day
  from $dws_location_bssid_info_di
  where day >= '$p1month'
  and day <= '$day'
  and plat = 1
  and real_date >= '$p1month'
  and real_date <= '${day} 23:59:59'
  group by device,bssid,day
) b on a.device=b.device
where a.day='$day';
"
