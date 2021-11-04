#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi


#月末运行
day_pre=$1
day=`date +%Y%m%d -d "${day_pre:0:6}01 -1 days"`

before365Day=`date +%Y%m%d -d "${day} -365 days"`


source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
master_reserved_new=$dws_device_install_app_re_status_di
tmpdb=$dm_mobdi_tmp
master_reserved_new_month_aggregation_mi=$tmpdb.md_master_reserved_new_month_aggregation_mi
device_pkg_name_master_resrved_min_day_mi=$tmpdb.md_device_pkg_name_master_resrved_min_day_mi

## 目标表
device_pkg_install_uninstall_year_info_mf=$label_device_pkg_install_uninstall_year_info_mf

#取master_reserved_new_month_aggregation_mi表12个分区数据，作为1年数据
hive -v -e "
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;

insert overwrite table $device_pkg_install_uninstall_year_info_mf partition(day='$day')
select a1.device,a1.pkg,name,
       case
         when refine_final_flag in (0,1) and flag_day <> update_day then -1
         else refine_final_flag
       end as refine_final_flag,
       first_day,
       case
         when refine_final_flag in (0,1) and flag_day <> update_day then nextday
         else flag_day
       end as flag_day,
       update_day
from
(
  select device,pkg,refine_final_flag,data_day as flag_day,
         max(data_day) over (partition by device) as update_day
  from
  (
    select device,pkg,refine_final_flag,data_day,
           row_number() over (partition by device,pkg order by data_day desc) as rk
    from $master_reserved_new_month_aggregation_mi
    where day>'$before365Day'
    and day<='$day'
  ) a
  where rk = 1
) a1
inner join
(
  select device,data_day,
         lag(data_day,1) over (partition by device order by data_day desc) as nextday
  from
  (
    select device,day as data_day
    from $master_reserved_new
    where day>'$before365Day'
    and day<='$day'
    group by device,day
  ) a
) a2 on a1.device = a2.device and a1.flag_day = a2.data_day
inner join
(
  select device,pkg,name,day as first_day
  from $device_pkg_name_master_resrved_min_day_mi
  where day<='$day'
) a3 on a1.device=a3.device and a1.pkg=a3.pkg
cluster by device;
"
