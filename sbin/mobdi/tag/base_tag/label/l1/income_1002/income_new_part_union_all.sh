#!/bin/bash

set -e -x

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=dm_mobdi_tmp
income_new_ratio_features_1m="${tmpdb}.income_new_ratio_features_1m"
income_new_ratio_features_3m="${tmpdb}.income_new_ratio_features_3m"
income_new_ratio_features_6m="${tmpdb}.income_new_ratio_features_6m"
income_new_ratio_features_12m="${tmpdb}.income_new_ratio_features_12m"
income_new_newIns_recency_features="${tmpdb}.income_new_newIns_recency_features"
income_new_Ins_recency_features="${tmpdb}.income_new_Ins_recency_features"
income_new_active_recency_features="${tmpdb}.income_new_active_recency_features"
income_new_topic_wgt="${tmpdb}.income_new_topic_wgt"
income_new_embedding_cosin_bycate="${tmpdb}.income_new_embedding_cosin_bycate"

# output
income_new_features_all="${tmpdb}.income_new_features_all"


HADOOP_USER_NAME=dba hive -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;


insert overwrite table  $income_new_features_all partition (day='$day')
select
  a.device
  ,a1_12m_cnt
  ,a10_12m_cnt
  ,a10_2_12m_cnt
  ,a10_3_12m_cnt
  ,a12_12m_cnt
  ,a19_12m_cnt
  ,a2_12m_cnt
  ,a3_12m_cnt
  ,a5_1_12m_cnt
  ,a8_1_12m_cnt
  ,a8_12m_cnt
  ,b1_12m_cnt
  ,b12_12m_cnt
  ,b4_12m_cnt
  ,b6_3_12m_cnt
  ,b8_1_12m_cnt
  ,b8_2_12m_cnt
  ,fin_48_12m_cnt
  ,total_12m_cnt

  ,a1_12m_ratio
  ,a10_12m_ratio
  ,a10_3_12m_ratio
  ,a13_12m_ratio
  ,a2_4_12m_ratio
  ,a8_12m_ratio
  ,b3_12m_ratio
  ,b4_12m_ratio
  ,b6_4_12m_ratio
  ,fin_48_12m_ratio
  ,tgi1_1_0_12m_ratio
  ,tgi1_1_1_12m_ratio
  ,tgi1_1_2_12m_ratio
  ,tgi1_1_3_12m_ratio
  ,tgi1_2_0_12m_ratio
  ,tgi1_3_0_12m_ratio
  ,tgi1_3_1_12m_ratio
  ,tgi1_3_2_12m_ratio
  ,tgi1_4_1_12m_ratio
  ,tgi1_4_2_12m_ratio
  ,tgi1_5_1_12m_ratio
  ,tgi1_5_2_12m_ratio
  ,tgi1_5_3_12m_ratio

  ,COALESCE(a12_6m_cnt, -999) as a12_6m_cnt
  ,COALESCE(a19_6m_cnt, -999) as a19_6m_cnt
  ,COALESCE(a2_6m_cnt, -999) as a2_6m_cnt
  ,COALESCE(a3_6m_cnt, -999) as a3_6m_cnt
  ,COALESCE(a8_1_6m_cnt, -999) as a8_1_6m_cnt
  ,COALESCE(a8_6m_cnt, -999) as a8_6m_cnt
  ,COALESCE(b1_6m_cnt, -999) as b1_6m_cnt
  ,COALESCE(b12_6m_cnt, -999) as b12_6m_cnt
  ,COALESCE(b8_1_6m_cnt, -999) as b8_1_6m_cnt
  ,COALESCE(fin_48_6m_cnt, -999) as fin_48_6m_cnt
  ,COALESCE(total_6m_cnt, -999) as total_6m_cnt

  ,COALESCE(a1_6m_ratio, -999) as a1_6m_ratio
  ,COALESCE(a10_6m_ratio, -999) as a10_6m_ratio
  ,COALESCE(a13_1_6m_ratio, -999) as a13_1_6m_ratio
  ,COALESCE(a2_4_6m_ratio, -999) as a2_4_6m_ratio
  ,COALESCE(a8_6m_ratio, -999) as a8_6m_ratio
  ,COALESCE(b3_6m_ratio, -999) as b3_6m_ratio
  ,COALESCE(fin_48_6m_ratio, -999) as fin_48_6m_ratio
  ,COALESCE(tgi1_1_0_6m_ratio, -999) as tgi1_1_0_6m_ratio
  ,COALESCE(tgi1_1_2_6m_ratio, -999) as tgi1_1_2_6m_ratio
  ,COALESCE(tgi1_1_3_6m_ratio, -999) as tgi1_1_3_6m_ratio
  ,COALESCE(tgi1_3_0_6m_ratio, -999) as tgi1_3_0_6m_ratio
  ,COALESCE(tgi1_3_1_6m_ratio, -999) as tgi1_3_1_6m_ratio
  ,COALESCE(tgi1_3_2_6m_ratio, -999) as tgi1_3_2_6m_ratio
  ,COALESCE(tgi1_4_1_6m_ratio, -999) as tgi1_4_1_6m_ratio
  ,COALESCE(tgi1_4_2_6m_ratio, -999) as tgi1_4_2_6m_ratio
  ,COALESCE(tgi1_5_2_6m_ratio, -999) as tgi1_5_2_6m_ratio
  ,COALESCE(tgi1_5_3_6m_ratio, -999) as tgi1_5_3_6m_ratio

  ,COALESCE(a19_3m_cnt, -999) as a19_3m_cnt
  ,COALESCE(b1_3m_cnt, -999) as b1_3m_cnt
  ,COALESCE(b12_3m_cnt, -999) as b12_3m_cnt
  ,COALESCE(a1_3m_ratio, -999) as a1_3m_ratio
  ,COALESCE(a10_3m_ratio, -999) as a10_3m_ratio
  ,COALESCE(a13_1_3m_ratio, -999) as a13_1_3m_ratio
  ,COALESCE(a13_3m_ratio, -999) as a13_3m_ratio
  ,COALESCE(a2_4_3m_ratio, -999) as a2_4_3m_ratio
  ,COALESCE(fin_48_3m_ratio, -999) as fin_48_3m_ratio
  ,COALESCE(tgi1_1_0_3m_ratio, -999) as tgi1_1_0_3m_ratio
  ,COALESCE(tgi1_1_2_3m_ratio, -999) as tgi1_1_2_3m_ratio
  ,COALESCE(tgi1_1_3_3m_ratio, -999) as tgi1_1_3_3m_ratio
  ,COALESCE(tgi1_2_0_3m_ratio, -999) as tgi1_2_0_3m_ratio
  ,COALESCE(tgi1_2_1_3m_ratio, -999) as tgi1_2_1_3m_ratio
  ,COALESCE(tgi1_3_0_3m_ratio, -999) as tgi1_3_0_3m_ratio
  ,COALESCE(tgi1_3_2_3m_ratio, -999) as tgi1_3_2_3m_ratio
  ,COALESCE(tgi1_5_2_3m_ratio, -999) as tgi1_5_2_3m_ratio
  ,COALESCE(tgi1_5_3_3m_ratio, -999) as tgi1_5_3_3m_ratio

  ,COALESCE(a12_1m_cnt, -999) as a12_1m_cnt
  ,COALESCE(tgi1_1_0_1m_ratio, -999) as tgi1_1_0_1m_ratio
  ,COALESCE(tgi1_1_2_1m_ratio, -999) as tgi1_1_2_1m_ratio
  ,COALESCE(tgi1_1_3_1m_ratio, -999) as tgi1_1_3_1m_ratio
  ,COALESCE(tgi1_2_1_1m_ratio, -999) as tgi1_2_1_1m_ratio
  ,COALESCE(tgi1_3_0_1m_ratio, -999) as tgi1_3_0_1m_ratio

  ,COALESCE(a19_newins_first_min_diff, -99) as a19_newins_first_min_diff
  ,COALESCE(tgi1_1_0_newins_first_min_diff, -99) as tgi1_1_0_newins_first_min_diff
  ,COALESCE(tgi1_2_0_newins_first_min_diff, -99) as tgi1_2_0_newins_first_min_diff
  ,COALESCE(tgi1_3_2_newins_first_max_diff, -99) as tgi1_3_2_newins_first_max_diff
  ,COALESCE(tgi1_5_2_newins_first_min_diff, -99) as tgi1_5_2_newins_first_min_diff

  ,COALESCE(a1_install_first_max_diff, -99) as a1_install_first_max_diff
  ,COALESCE(a10_2_install_first_max_diff, -99) as a10_2_install_first_max_diff
  ,COALESCE(a12_install_first_max_diff, -99) as a12_install_first_max_diff
  ,COALESCE(a19_install_first_max_diff, -99) as a19_install_first_max_diff
  ,COALESCE(fin_46_install_first_max_diff, -99) as fin_46_install_first_max_diff
  ,COALESCE(tgi1_1_0_install_first_max_diff, -99) as tgi1_1_0_install_first_max_diff
  ,COALESCE(tgi1_1_3_install_first_max_diff, -99) as tgi1_1_3_install_first_max_diff
  ,COALESCE(tgi1_5_2_install_first_max_diff, -99) as tgi1_5_2_install_first_max_diff

  ,COALESCE(a1_active_max_day_diff, -95) as a1_active_max_day_diff
  ,COALESCE(a12_active_max_day_diff, -95) as a12_active_max_day_diff
  ,COALESCE(a19_active_max_day_diff, -95) as a19_active_max_day_diff
  ,COALESCE(fin_46_active_min_day_diff, -95) as fin_46_active_min_day_diff
  ,COALESCE(tgi1_1_0_active_max_day_diff, -95) as tgi1_1_0_active_max_day_diff
  ,COALESCE(tgi1_1_3_active_min_day_diff, -95) as tgi1_1_3_active_min_day_diff
  ,COALESCE(tgi1_2_0_active_max_day_diff, -95) as tgi1_2_0_active_max_day_diff
  ,COALESCE(tgi1_3_2_active_max_day_diff, -95) as tgi1_3_2_active_max_day_diff
  ,COALESCE(tgi1_4_1_active_max_day_diff, -95) as tgi1_4_1_active_max_day_diff
  ,COALESCE(tgi1_4_2_active_max_day_diff, -95) as tgi1_4_2_active_max_day_diff
  ,COALESCE(tgi1_5_2_active_min_day_diff, -95) as tgi1_5_2_active_min_day_diff
  ,COALESCE(tgi1_5_3_active_max_day_diff, -95) as tgi1_5_3_active_max_day_diff

  ,COALESCE(a10_1_income1_install_trend, -999) as a10_1_income1_install_trend
  ,COALESCE(a10_3_income1_install_trend, -999) as a10_3_income1_install_trend
  ,COALESCE(a11_income1_install_trend, -999) as a11_income1_install_trend
  ,COALESCE(a12_income1_install_trend, -999) as a12_income1_install_trend
  ,COALESCE(a13_2_income1_install_trend, -999) as a13_2_income1_install_trend
  ,COALESCE(a13_4_income1_install_trend, -999) as a13_4_income1_install_trend
  ,COALESCE(a14_3_income1_install_trend, -999) as a14_3_income1_install_trend
  ,COALESCE(a15_income1_install_trend, -999) as a15_income1_install_trend
  ,COALESCE(a16_income1_install_trend, -999) as a16_income1_install_trend
  ,COALESCE(a17_income1_install_trend, -999) as a17_income1_install_trend
  ,COALESCE(a2_5_income1_install_trend, -999) as a2_5_income1_install_trend
  ,COALESCE(a2_6_income1_install_trend, -999) as a2_6_income1_install_trend
  ,COALESCE(a2_8_income1_install_trend, -999) as a2_8_income1_install_trend
  ,COALESCE(a2_income1_install_trend, -999) as a2_income1_install_trend
  ,COALESCE(a20_12_income1_install_trend, -999) as a20_12_income1_install_trend
  ,COALESCE(a20_5_income1_install_trend, -999) as a20_5_income1_install_trend
  ,COALESCE(a20_6_income1_install_trend, -999) as a20_6_income1_install_trend
  ,COALESCE(a20_7_income1_install_trend, -999) as a20_7_income1_install_trend
  ,COALESCE(a20_income1_install_trend, -999) as a20_income1_install_trend
  ,COALESCE(a4_1_income1_install_trend, -999) as a4_1_income1_install_trend
  ,COALESCE(a4_3_income1_install_trend, -999) as a4_3_income1_install_trend
  ,COALESCE(a5_3_income1_install_trend, -999) as a5_3_income1_install_trend
  ,COALESCE(a5_income1_install_trend, -999) as a5_income1_install_trend
  ,COALESCE(a6_3_income1_install_trend, -999) as a6_3_income1_install_trend
  ,COALESCE(a7_2_income1_install_trend, -999) as a7_2_income1_install_trend
  ,COALESCE(a7_3_income1_install_trend, -999) as a7_3_income1_install_trend
  ,COALESCE(a8_3_income1_install_trend, -999) as a8_3_income1_install_trend
  ,COALESCE(a8_income1_install_trend, -999) as a8_income1_install_trend
  ,COALESCE(a9_2_income1_install_trend, -999) as a9_2_income1_install_trend
  ,COALESCE(b1_income1_install_trend, -999) as b1_income1_install_trend
  ,COALESCE(b3_income1_install_trend, -999) as b3_income1_install_trend
  ,COALESCE(b4_income1_install_trend, -999) as b4_income1_install_trend
  ,COALESCE(b6_4_income1_install_trend, -999) as b6_4_income1_install_trend
  ,COALESCE(b6_income1_install_trend, -999) as b6_income1_install_trend
  ,COALESCE(b7_1_income1_install_trend, -999) as b7_1_income1_install_trend
  ,COALESCE(b7_2_income1_install_trend, -999) as b7_2_income1_install_trend
  ,COALESCE(b8_1_income1_install_trend, -999) as b8_1_income1_install_trend
  ,COALESCE(b8_income1_install_trend, -999) as b8_income1_install_trend
  ,COALESCE(fin_10_income1_install_trend, -999) as fin_10_income1_install_trend
  ,COALESCE(fin_13_income1_install_trend, -999) as fin_13_income1_install_trend
  ,COALESCE(fin_14_income1_install_trend, -999) as fin_14_income1_install_trend
  ,COALESCE(fin_17_income1_install_trend, -999) as fin_17_income1_install_trend
  ,COALESCE(fin_18_income1_install_trend, -999) as fin_18_income1_install_trend
  ,COALESCE(fin_2_income1_install_trend, -999) as fin_2_income1_install_trend
  ,COALESCE(fin_21_income1_install_trend, -999) as fin_21_income1_install_trend
  ,COALESCE(fin_22_income1_install_trend, -999) as fin_22_income1_install_trend
  ,COALESCE(fin_26_income1_install_trend, -999) as fin_26_income1_install_trend
  ,COALESCE(fin_28_income1_install_trend, -999) as fin_28_income1_install_trend
  ,COALESCE(fin_31_income1_install_trend, -999) as fin_31_income1_install_trend
  ,COALESCE(fin_33_income1_install_trend, -999) as fin_33_income1_install_trend
  ,COALESCE(fin_41_income1_install_trend, -999) as fin_41_income1_install_trend
  ,COALESCE(fin_42_income1_install_trend, -999) as fin_42_income1_install_trend
  ,COALESCE(fin_48_income1_install_trend, -999) as fin_48_income1_install_trend
  ,COALESCE(fin_50_income1_install_trend, -999) as fin_50_income1_install_trend
  ,COALESCE(fin_51_income1_install_trend, -999) as fin_51_income1_install_trend
  ,COALESCE(fin_52_income1_install_trend, -999) as fin_52_income1_install_trend
  ,COALESCE(fin_6_income1_install_trend, -999) as fin_6_income1_install_trend
  ,COALESCE(tgi1_1_2_income1_install_trend, -999) as tgi1_1_2_income1_install_trend
  ,COALESCE(tgi1_1_3_income1_install_trend, -999) as tgi1_1_3_income1_install_trend
  ,COALESCE(tgi1_2_0_income1_install_trend, -999) as tgi1_2_0_income1_install_trend
  ,COALESCE(tgi1_2_3_income1_install_trend, -999) as tgi1_2_3_income1_install_trend
  ,COALESCE(tgi1_3_0_income1_install_trend, -999) as tgi1_3_0_income1_install_trend
  ,COALESCE(tgi1_3_1_income1_install_trend, -999) as tgi1_3_1_income1_install_trend
  ,COALESCE(tgi1_3_2_income1_install_trend, -999) as tgi1_3_2_income1_install_trend
  ,COALESCE(tgi1_4_2_income1_install_trend, -999) as tgi1_4_2_income1_install_trend
  ,COALESCE(topic_0, -999) as topic_0
  ,COALESCE(topic_1, -999) as topic_1
  ,COALESCE(topic_11, -999) as topic_11
  ,COALESCE(topic_16, -999) as topic_16
  ,COALESCE(topic_7, -999) as topic_7
  ,COALESCE(topic_8, -999) as topic_8
  ,COALESCE(topic_9, -999) as topic_9
from(
    select *
    from $income_new_ratio_features_12m
    where day='$day'
)a 
left join(
    select *
    from $income_new_ratio_features_6m
    where day='$day'
)b 
on a.device = b.device
left join(
    select *
    from $income_new_ratio_features_3m
    where day='$day'
)c 
on a.device = c.device
left join(
    select *
    from $income_new_ratio_features_1m
    where day='$day'
)d 
on a.device =d.device
left join(
    select *
    from $income_new_newIns_recency_features
    where day='$day'
)e 
on a.device = e.device
left join(
    select *
    from $income_new_Ins_recency_features
    where day='$day'
)f
on a.device = f.device
left join(
    select *
    from $income_new_active_recency_features
    where day='$day'
)g
on a.device = g.device
left join(
    select *
    from $income_new_topic_wgt
    where day='$day'
)k
on a.device = k.device
left join(
    select *
    from $income_new_embedding_cosin_bycate
    where day='$day'
)l
on a.device = l.device
"