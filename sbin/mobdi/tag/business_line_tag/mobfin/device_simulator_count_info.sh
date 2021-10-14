#!/bin/sh

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

day=$1
p1week=`date -d "$day -7 day" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
#dwd_simulator_det_info_sec_di=dm_mobdi_master.dwd_simulator_det_info_sec_di

## 目标表
#label_l1_anticheat_device_simulator_wi=dm_mobdi_report.label_l1_anticheat_device_simulator_wi

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

insert overwrite table $label_l1_anticheat_device_simulator_wi partition(day='$day')
select deviceid as device, count(*) as cnt_7
from
( select deviceid,day
  from $dwd_simulator_det_info_sec_di
  where day > '$p1week'
  and day <= '$day'
  and ( qemukernel != 0 or qemuFileExist = true or qemuDevExist = true or blueStacksFileExist = true )
  group by deviceid,day
) t
group by deviceid
;
"
