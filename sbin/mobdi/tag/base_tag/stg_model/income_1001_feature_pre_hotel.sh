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
hotel_bssid=dw_mobdi_md.hotel_bssid
income_1001_bssid_index_calculate_base_info=${tmp_db}.income_1001_bssid_index_calculate_base_info

#out
income_1001_hotel_bssid_index=${tmp_db}.income_1001_hotel_bssid_index

#计算酒店bssid特征
#通过还未上线的test.qingy_hotel_bssid_ddp酒店bssid表，先计算device在每个连接日期中最高的价格等级、最高的星级、最高的评分等级
#计算设备是否去过各个价格等级、星级、评分等级的酒店
#根据上一步结果计算index
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $income_1001_hotel_bssid_index partition(day='$day')
select device,
       case
         when price_level1 =0 then 18
         when price_level1 >0 then 19
       end as price_level1_index,
       case
         when price_level2 =0 then 20
         when price_level2 >0 then 21
       end as price_level2_index,
       case
         when price_level3 =0 then 22
         when price_level3 >0 then 23
       end as price_level3_index,
       case
         when price_level4 =0 then 24
         when price_level4 >0 then 25
       end as price_level4_index,
       case
         when price_level5 =0 then 26
         when price_level5 >0 then 27
       end as price_level5_index,
       case
         when price_level6 =0 then 28
         when price_level6 >0 then 29
       end as price_level6_index,
       case
         when rank_star1 =0 then 30
         when rank_star1 >0 then 31
       end as rank_star1_index,
       case
         when rank_star2 =0 then 32
         when rank_star2 >0 then 33
       end as rank_star2_index,
       case
         when rank_star3 =0 then 34
         when rank_star3 >0 then 35
       end as rank_star3_index,
       case
         when rank_star4 =0 then 36
         when rank_star4 >0 then 37
       end as rank_star4_index,
       case
         when rank_star5 =0 then 38
         when rank_star5 >0 then 39
       end as rank_star5_index,
       case
         when score_type1 =0 then 40
         when score_type1 >0 then 41
       end as score_type1_index,
       case
         when score_type2 =0 then 42
         when score_type2 >0 then 43
       end as score_type2_index,
       case
         when score_type3 =0 then 44
         when score_type3 >0 then 45
       end as score_type3_index
from
(
  select device,
         sum(case when price_level=1 then 1 else 0 end) as price_level1,
         sum(case when price_level=2 then 1 else 0 end) as price_level2,
         sum(case when price_level=3 then 1 else 0 end) as price_level3,
         sum(case when price_level=4 then 1 else 0 end) as price_level4,
         sum(case when price_level=5 then 1 else 0 end) as price_level5,
         sum(case when price_level=6 then 1 else 0 end) as price_level6,
         sum(case when rank_star=1 then 1 else 0 end) as rank_star1,
         sum(case when rank_star=2 then 1 else 0 end) as rank_star2,
         sum(case when rank_star=3 then 1 else 0 end) as rank_star3,
         sum(case when rank_star=4 then 1 else 0 end) as rank_star4,
         sum(case when rank_star=5 then 1 else 0 end) as rank_star5,
         sum(case when score_type=1 then 1 else 0 end) as score_type1,
         sum(case when score_type=2 then 1 else 0 end) as score_type2,
         sum(case when score_type=3 then 1 else 0 end) as score_type3
  from
  (
    select device,active_day,
           max(price_level) as price_level,
           max(rank_star) as rank_star,
           max(score_type) as score_type
    from
    (
      select a.device,b.active_day,price_level,
             case
               when rank_star='无星级' then 1
               when rank_star='二星/经济' then 2
               when rank_star='三星/舒适' then 3
               when rank_star='四星/高档' then 4
               when rank_star='五星/豪华' then 5
               else 0
             end as rank_star,
             case
               when score_type='评分较低' then 1
               when score_type='评价中等' then 2
               when score_type='评分较高' then 3
             else 0 end as score_type
      from $calculate_model_device a
      left join
      (
        select b.device,b.active_day,a.price_level,a.rank_star,a.score_type
        from $hotel_bssid a
        inner join
        $income_1001_bssid_index_calculate_base_info b on b.day='$day' and a.bssid=b.bssid
      ) b on a.device=b.device
      where a.day='$day'
    ) a
    group by device,active_day
  ) t1
  group by device
) t1;
"
