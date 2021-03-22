#!/bin/sh

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

day=$1

p1week=`date -d "$day -7 day" +%Y%m%d`
p2weeks=`date -d "$day -14 day" +%Y%m%d`
p1month=`date -d "$day -30 day" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

## 源表
#rp_device_profile_full_view=dm_mobdi_report.rp_device_profile_full_view

## 目标表
#label_l1_anticheat_device_break_wi=dm_mobdi_report.label_l1_anticheat_device_break_wi

hive -v -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=256000000;

insert overwrite table $label_l1_anticheat_device_break_wi partition(day='$day', timewindow='7', flag = 0)
select
  device
from $rp_device_profile_full_view
where last_active >= '$p1week' and breaked = 'true'
;

insert overwrite table $label_l1_anticheat_device_break_wi partition(day='$day', timewindow='14', flag = 0)
select
  device
from $rp_device_profile_full_view
where last_active >= '$p2weeks' and breaked = 'true'
;

insert overwrite table $label_l1_anticheat_device_break_wi partition(day='$day', timewindow='30', flag = 0)
select
  device
from $rp_device_profile_full_view
where last_active >= '$p1month' and breaked = 'true'
;
"

