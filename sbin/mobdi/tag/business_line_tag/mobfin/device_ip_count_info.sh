#!/bin/sh

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

day=$1
p1week=`date -d "$day -7 day" +%Y%m%d`
p1month=`date -d "$day -30 day" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#输入表
#dws_device_ip_info_di=dm_mobdi_master.dws_device_ip_info_di

## 目标表
#label_l1_anticheat_device_ip_wi=dm_mobdi_report.label_l1_anticheat_device_ip_wi

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

insert overwrite table $label_l1_anticheat_device_ip_wi partition (day='$day')
select
  t1.device,
  nvl(ipcnt_1, 0) as ipcnt_1,
  nvl(ipcnt_7, 0) as ipcnt_7,
  nvl(ipcnt_30, 0) as ipcnt_30
from
( select device,count(1) as ipcnt_30
  from
       ( select
           device, ipaddr
         from $dws_device_ip_info_di
         where day > '$p1month' 
		 and day <= '$day'
         group by device,ipaddr
       ) month_cnt
  group by device
) t1
left join
( select device,count(1) as ipcnt_7
  from
       ( select
           device, ipaddr
         from $dws_device_ip_info_di
         where day > '$p1week' 
		 and day <= '$day'
         group by device,ipaddr
       ) week_cnt
  group by device
) t2
on t1.device = t2.device
left join
( select device,count(1) as ipcnt_1
  from
       ( select
           device, ipaddr
         from $dws_device_ip_info_di
         where day = '$day'
         group by device,ipaddr
       ) day_cnt
  group by device
) t3
on t1.device = t3.device
;
"




