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

#input
calculate_model_device=${tmp_db}.calculate_model_device
income_1001_bssid_index_calculate_base_info=${tmp_db}.income_1001_bssid_index_calculate_base_info

#mapping
#dim_shopping_mall_ssid_bssid_match_info_mf=dim_mobdi_mapping.dim_shopping_mall_ssid_bssid_match_info_mf

#out
income_1001_shopping_mall_bssid_index=${tmp_db}.income_1001_shopping_mall_bssid_index

shoppingMallBssidMappingLastPar=hive -e "show partitions $dim_shopping_mall_ssid_bssid_match_info_mf" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 1
#计算商场bssid特征
#通过dm_mobdi_mapping.dim_shopping_mall_ssid_bssid_match_info_mf表，先计算device连接商场的日期
#然后计算device在商场的连接天数
#根据连接天数计算index
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $income_1001_shopping_mall_bssid_index partition(day='$day')
select t1.device,
       case
         when day_cnt > 0 and day_cnt <= 5 then 5
         when day_cnt > 5 and day_cnt <= 10 then 6
         when day_cnt > 10 and day_cnt <= 20 then 7
         when day_cnt > 20 then 8
         else 9
       end as index
from $calculate_model_device t1
left join
(
  select device, count(1) as day_cnt
  from
  (
    select b.device,b.active_day
    from
    (
      select bssid
      from $dim_shopping_mall_ssid_bssid_match_info_mf
      lateral view explode(bssid_array) n as bssid
      where day='$shoppingMallBssidMappingLastPar'
      group by bssid
    ) a
    inner join
    $income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=b.bssid
    group by b.device,b.active_day
  ) a
  group by device
) t2 on t1.device=t2.device
where t1.day='$day';
"
