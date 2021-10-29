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

tmp_db=$dm_mobdi_tmp

input
#calculate_model_device=${tmp_db}.calculate_model_device

#md
traffic_bssid=dw_mobdi_md.traffic_bssid
income_1001_bssid_index_calculate_base_info=${tmp_db}.income_1001_bssid_index_calculate_base_info

#out
income_1001_traffic_bssid_index=${tmp_db}.income_1001_traffic_bssid_index


#计算交通bssid特征
#通过还未上线的test.qingy_traffic_bssid_dpp交通bssid表，先计算device连接交通的类型和连接日期
#然后计算device是否连接过长途汽车站、地铁站、机场、火车站
#根据连接结果计算index
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $income_1001_traffic_bssid_index partition(day='$day')
select device,
       case
         when traffic_bus=0 then 10
         when traffic_bus>0 then 11
       end as traffic_bus_index,
       case
         when traffic_subway=0 then 12
         when traffic_subway>0 then 13
       end as traffic_subway_index,
       case
         when traffic_airport=0 then 14
         when traffic_airport>0 then 15
       end as traffic_airport_index,
       case
         when traffic_train=0 then 16
         when traffic_train>0 then 17
       end as traffic_train_index
from
(
  select device,
         sum(case when type_name='长途汽车站' then 1 else 0 end) as traffic_bus,
         sum(case when type_name='地铁站' then 1 else 0 end) as traffic_subway,
         sum(case when type_name='机场' then 1 else 0 end) as traffic_airport,
         sum(case when type_name='火车站' then 1 else 0 end) as traffic_train
  from
  (
    select a.device,b.active_day,b.type_name
    from $calculate_model_device a
    left join
    (
      select b.device,b.active_day,a.type_name
      from $traffic_bssid a
      inner join
      $income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=b.bssid
    ) b on a.device=b.device
    where a.day='$day'
    group by a.device,b.active_day,b.type_name
  ) a
  group by device
) t1;
"