#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

#月末运行
pre_day1=$1
pre_day2=`date -d $pre_day1 +%Y%m01`
day=`date +%Y%m%d -d "${pre_day2} -1 day"`

monthFirstDay=`date -d $day +%Y%m01`

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

## 源表
master_reserved_new=${dws_device_install_app_re_status_di}

## 输出
master_reserved_new_month_aggregation_mi=dw_mobdi_tmp.md_master_reserved_new_month_aggregation_mi

#聚合一个月的master_reserved_new数据，存储起来
hive -v -e "
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $master_reserved_new_month_aggregation_mi partition(day='$day')
select device,pkg,refine_final_flag,day as data_day
from
(
  select device,pkg,refine_final_flag,day,
         row_number() over (partition by device,pkg order by day desc) as rk
  from $master_reserved_new
  where day>='$monthFirstDay'
  and day<='$day'
) t1
where rk = 1
cluster by device;
"
