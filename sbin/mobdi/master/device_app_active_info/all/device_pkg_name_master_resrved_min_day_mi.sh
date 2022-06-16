#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

pre_day1=$1
pre_day2=`date -d $pre_day1 +%Y%m01`


#月末运行
day=`date +%Y%m%d -d "${pre_day2} -1 day"`
monthFirstDay=`date -d $day +%Y%m01`

source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
master_reserved_new=${dws_device_install_app_re_status_di}

tmpdb=$dm_mobdi_tmp
## 临时表
calculate_tmp=$tmpdb.tmp_calculate

## 目标表
device_pkg_name_master_resrved_min_day_mi=$tmpdb.md_device_pkg_name_master_resrved_min_day_mi

#如果下列不拆成两句sql，会出现严重的数据倾斜现象
hive -v -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

drop table if exists $calculate_tmp;
create table $calculate_tmp stored AS TEXTFILE as
select t1.device,t1.pkg,null as name,t1.day
from
(
  select device,pkg,min(day) as day
  from $master_reserved_new
  where day>='$monthFirstDay' and day<='$day'
  group by device,pkg
) t1
left join
$device_pkg_name_master_resrved_min_day_mi t2 
on t2.day<'$monthFirstDay' and t1.device=t2.device and t1.pkg=t2.pkg
where t2.device is null
cluster by t1.device;

insert overwrite table $device_pkg_name_master_resrved_min_day_mi partition(day)
select device,t1.pkg,name,day
from
(
  select pkg,name
  from dm_sdk_mapping.pkg_name_mapping
) t1
inner join
(
  select device,pkg,day
  from $calculate_tmp
  where day>='$monthFirstDay' and day<='$day'
) t2 on t1.pkg=t2.pkg
cluster by device;
"
