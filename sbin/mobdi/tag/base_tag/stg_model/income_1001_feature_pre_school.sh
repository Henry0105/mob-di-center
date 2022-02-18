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
poi_school=dw_mobdi_md.poi_school

#md
income_1001_bssid_index_calculate_base_info=${tmp_db}.income_1001_bssid_index_calculate_base_info

#out
income_1001_university_bssid_index=${tmp_db}.income_1001_university_bssid_index

#计算学校bssid特征
#通过还未上线的test.hejy_temp_poi_school_15学校bssid对应表，先计算device在各学校bssid的连接日期
#然后计算device在各学校bssid的连接天数
#对每个device保留过去一个月连接最频繁的学校对应的连接天数
#根据连接天数计算index
hive -v -e "
set hive.auto.convert.join=false;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $income_1001_university_bssid_index partition(day='$day')
select device,
       case
         when university_connect_day_cnt > 0 and university_connect_day_cnt <= 10 then 1
         when university_connect_day_cnt > 10 and university_connect_day_cnt <= 20 then 2
         when university_connect_day_cnt > 20 then 3
         else 4
       end as index
from
(
  select device,university_connect_day_cnt
  from
  (
    select device,university_name,university_connect_day_cnt,
           row_number() over(partition by device order by university_connect_day_cnt desc) as rank
    from
    (
      select device,university_name,count(1) as university_connect_day_cnt
      from
      (
        select b.device,a.university_name,b.active_day
        from $poi_school a
        inner join
        $income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=regexp_replace(b.bssid,':','')
        group by b.device,a.university_name,b.active_day
      ) t
      group by device,university_name
    ) a
  ) b
  where rank=1
) t;
"