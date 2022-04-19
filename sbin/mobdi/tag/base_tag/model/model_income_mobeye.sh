#!/bin/bash
set -x -e

source /home/dba/mobdi_center/conf/hive-env.sh

#月更，每个月最后一天
day=$1
day_before_one_month=$(date -d $day  "+%Y%m01")

#近12个月每个设备的各个类别app的个数和占比
income_temp_category_mapping="dm_mobeye_master.income_temp_category_mapping"
label_device_pkg_install_uninstall_year_info_mf="dm_mobdi_report.label_device_pkg_install_uninstall_year_info_mf"
income_new_uninstall_avg_embedding_cosin_tmp="dm_mobdi_tmp.income_new_uninstall_avg_embedding_cosin_tmp"

device_profile_label_full_par="dm_mobdi_report.device_profile_label_full_par"

income_temp_features_all="dm_mobdi_tmp.income_temp_features_all"

device_profile_lable_income="dm_mobeye_master.device_profile_lable_income"

last_par=$(hive -e "show partitions $device_profile_label_full_par"|grep -v '_bak'|tail -n 1| awk -F '=' '{print $2}')

#1.app在装列表衍生特征

hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
SET hive.exec.parallel=true;
with raw_table as (
select * from (
   select device
    ,from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd') as date
    ,a.cate_id
    ,a.pkg
    ,b.refine_final_flag
    ,from_unixtime(unix_timestamp(b.first_day,'yyyyMMdd'),'yyyy-MM-dd') as first_day
    ,from_unixtime(unix_timestamp(b.flag_day,'yyyyMMdd'),'yyyy-MM-dd') as flag_day
    ,from_unixtime(unix_timestamp(b.update_day,'yyyyMMdd'),'yyyy-MM-dd') as update_day
 from(
  select pkg, cate_id
  from  $income_temp_category_mapping
  group by pkg, cate_id
 )a
 join(
  select device,pkg,refine_final_flag,first_day,flag_day,update_day,day
  from $label_device_pkg_install_uninstall_year_info_mf
  where day=${day} and update_day between ${day_before_one_month} and ${day}
 )b
 on a.pkg=b.pkg
 )t  where datediff(date,flag_day)<=365
),
income_temp_cate_embedding_cosin as (
select device
    ,max(case when cate_id='A1' then cosin_similarity else -1 end) as A1_trend
    ,max(case when cate_id='A10' then cosin_similarity else -1 end) as A10_trend
    ,max(case when cate_id='A10_1' then cosin_similarity else -1 end) as A10_1_trend
    ,max(case when cate_id='A10_2' then cosin_similarity else -1 end) as A10_2_trend
    ,max(case when cate_id='A10_3' then cosin_similarity else -1 end) as A10_3_trend
    ,max(case when cate_id='A11' then cosin_similarity else -1 end) as A11_trend
    ,max(case when cate_id='A12' then cosin_similarity else -1 end) as A12_trend
    ,max(case when cate_id='A13' then cosin_similarity else -1 end) as A13_trend
    ,max(case when cate_id='A13_1' then cosin_similarity else -1 end) as A13_1_trend
    ,max(case when cate_id='A13_2' then cosin_similarity else -1 end) as A13_2_trend
    ,max(case when cate_id='A13_3' then cosin_similarity else -1 end) as A13_3_trend
    ,max(case when cate_id='A13_4' then cosin_similarity else -1 end) as A13_4_trend
    ,max(case when cate_id='A14' then cosin_similarity else -1 end) as A14_trend
    ,max(case when cate_id='A14_1' then cosin_similarity else -1 end) as A14_1_trend
    ,max(case when cate_id='A14_3' then cosin_similarity else -1 end) as A14_3_trend
    ,max(case when cate_id='A15' then cosin_similarity else -1 end) as A15_trend
    ,max(case when cate_id='A17' then cosin_similarity else -1 end) as A17_trend
    ,max(case when cate_id='A18' then cosin_similarity else -1 end) as A18_trend
    ,max(case when cate_id='A19' then cosin_similarity else -1 end) as A19_trend
    ,max(case when cate_id='A2' then cosin_similarity else -1 end) as A2_trend
    ,max(case when cate_id='A20' then cosin_similarity else -1 end) as A20_trend
    ,max(case when cate_id='A20_1' then cosin_similarity else -1 end) as A20_1_trend
    ,max(case when cate_id='A20_10' then cosin_similarity else -1 end) as A20_10_trend
    ,max(case when cate_id='A20_11' then cosin_similarity else -1 end) as A20_11_trend
    ,max(case when cate_id='A20_12' then cosin_similarity else -1 end) as A20_12_trend
    ,max(case when cate_id='A20_2' then cosin_similarity else -1 end) as A20_2_trend
    ,max(case when cate_id='A20_3' then cosin_similarity else -1 end) as A20_3_trend
    ,max(case when cate_id='A20_4' then cosin_similarity else -1 end) as A20_4_trend
    ,max(case when cate_id='A20_5' then cosin_similarity else -1 end) as A20_5_trend
    ,max(case when cate_id='A20_6' then cosin_similarity else -1 end) as A20_6_trend
    ,max(case when cate_id='A20_7' then cosin_similarity else -1 end) as A20_7_trend
    ,max(case when cate_id='A20_8' then cosin_similarity else -1 end) as A20_8_trend
    ,max(case when cate_id='A20_9' then cosin_similarity else -1 end) as A20_9_trend
    ,max(case when cate_id='A2_1' then cosin_similarity else -1 end) as A2_1_trend
    ,max(case when cate_id='A2_2' then cosin_similarity else -1 end) as A2_2_trend
    ,max(case when cate_id='A2_3' then cosin_similarity else -1 end) as A2_3_trend
    ,max(case when cate_id='A2_4' then cosin_similarity else -1 end) as A2_4_trend
    ,max(case when cate_id='A2_5' then cosin_similarity else -1 end) as A2_5_trend
    ,max(case when cate_id='A2_6' then cosin_similarity else -1 end) as A2_6_trend
    ,max(case when cate_id='A2_7' then cosin_similarity else -1 end) as A2_7_trend
    ,max(case when cate_id='A2_8' then cosin_similarity else -1 end) as A2_8_trend
    ,max(case when cate_id='A3' then cosin_similarity else -1 end) as A3_trend
    ,max(case when cate_id='A4' then cosin_similarity else -1 end) as A4_trend
    ,max(case when cate_id='A4_1' then cosin_similarity else -1 end) as A4_1_trend
    ,max(case when cate_id='A4_2' then cosin_similarity else -1 end) as A4_2_trend
    ,max(case when cate_id='A4_3' then cosin_similarity else -1 end) as A4_3_trend
    ,max(case when cate_id='A5' then cosin_similarity else -1 end) as A5_trend
    ,max(case when cate_id='A5_1' then cosin_similarity else -1 end) as A5_1_trend
    ,max(case when cate_id='A5_2' then cosin_similarity else -1 end) as A5_2_trend
    ,max(case when cate_id='A5_3' then cosin_similarity else -1 end) as A5_3_trend
    ,max(case when cate_id='A6' then cosin_similarity else -1 end) as A6_trend
    ,max(case when cate_id='A6_1' then cosin_similarity else -1 end) as A6_1_trend
    ,max(case when cate_id='A6_2' then cosin_similarity else -1 end) as A6_2_trend
    ,max(case when cate_id='A6_3' then cosin_similarity else -1 end) as A6_3_trend
    ,max(case when cate_id='A7' then cosin_similarity else -1 end) as A7_trend
    ,max(case when cate_id='A7_1' then cosin_similarity else -1 end) as A7_1_trend
    ,max(case when cate_id='A7_2' then cosin_similarity else -1 end) as A7_2_trend
    ,max(case when cate_id='A7_3' then cosin_similarity else -1 end) as A7_3_trend
    ,max(case when cate_id='A7_4' then cosin_similarity else -1 end) as A7_4_trend
    ,max(case when cate_id='A8' then cosin_similarity else -1 end) as A8_trend
    ,max(case when cate_id='A8_1' then cosin_similarity else -1 end) as A8_1_trend
    ,max(case when cate_id='A8_2' then cosin_similarity else -1 end) as A8_2_trend
    ,max(case when cate_id='A8_3' then cosin_similarity else -1 end) as A8_3_trend
    ,max(case when cate_id='A8_4' then cosin_similarity else -1 end) as A8_4_trend
    ,max(case when cate_id='A8_5' then cosin_similarity else -1 end) as A8_5_trend
    ,max(case when cate_id='A8_6' then cosin_similarity else -1 end) as A8_6_trend
    ,max(case when cate_id='A9' then cosin_similarity else -1 end) as A9_trend
    ,max(case when cate_id='A9_1' then cosin_similarity else -1 end) as A9_1_trend
    ,max(case when cate_id='A9_2' then cosin_similarity else -1 end) as A9_2_trend
    ,max(case when cate_id='A9_3' then cosin_similarity else -1 end) as A9_3_trend
    ,max(case when cate_id='A9_4' then cosin_similarity else -1 end) as A9_4_trend
    ,max(case when cate_id='B1' then cosin_similarity else -1 end) as B1_trend
    ,max(case when cate_id='B10' then cosin_similarity else -1 end) as B10_trend
    ,max(case when cate_id='B11' then cosin_similarity else -1 end) as B11_trend
    ,max(case when cate_id='B12' then cosin_similarity else -1 end) as B12_trend
    ,max(case when cate_id='B13' then cosin_similarity else -1 end) as B13_trend
    ,max(case when cate_id='B2' then cosin_similarity else -1 end) as B2_trend
    ,max(case when cate_id='B3' then cosin_similarity else -1 end) as B3_trend
    ,max(case when cate_id='B4' then cosin_similarity else -1 end) as B4_trend
    ,max(case when cate_id='B5' then cosin_similarity else -1 end) as B5_trend
    ,max(case when cate_id='B6' then cosin_similarity else -1 end) as B6_trend
    ,max(case when cate_id='B6_1' then cosin_similarity else -1 end) as B6_1_trend
    ,max(case when cate_id='B6_2' then cosin_similarity else -1 end) as B6_2_trend
    ,max(case when cate_id='B6_3' then cosin_similarity else -1 end) as B6_3_trend
    ,max(case when cate_id='B6_4' then cosin_similarity else -1 end) as B6_4_trend
    ,max(case when cate_id='B6_5' then cosin_similarity else -1 end) as B6_5_trend
    ,max(case when cate_id='B6_6' then cosin_similarity else -1 end) as B6_6_trend
    ,max(case when cate_id='B7' then cosin_similarity else -1 end) as B7_trend
    ,max(case when cate_id='B7_1' then cosin_similarity else -1 end) as B7_1_trend
    ,max(case when cate_id='B7_2' then cosin_similarity else -1 end) as B7_2_trend
    ,max(case when cate_id='B8' then cosin_similarity else -1 end) as B8_trend
    ,max(case when cate_id='B8_1' then cosin_similarity else -1 end) as B8_1_trend
    ,max(case when cate_id='B8_2' then cosin_similarity else -1 end) as B8_2_trend
    ,max(case when cate_id='B8_3' then cosin_similarity else -1 end) as B8_3_trend
    ,max(case when cate_id='B9' then cosin_similarity else -1 end) as B9_trend
    ,max(case when cate_id='fin_1' then cosin_similarity else -1 end) as fin_1_trend
    ,max(case when cate_id='fin_10' then cosin_similarity else -1 end) as fin_10_trend
    ,max(case when cate_id='fin_11' then cosin_similarity else -1 end) as fin_11_trend
    ,max(case when cate_id='fin_12' then cosin_similarity else -1 end) as fin_12_trend
    ,max(case when cate_id='fin_13' then cosin_similarity else -1 end) as fin_13_trend
    ,max(case when cate_id='fin_14' then cosin_similarity else -1 end) as fin_14_trend
    ,max(case when cate_id='fin_15' then cosin_similarity else -1 end) as fin_15_trend
    ,max(case when cate_id='fin_16' then cosin_similarity else -1 end) as fin_16_trend
    ,max(case when cate_id='fin_17' then cosin_similarity else -1 end) as fin_17_trend
    ,max(case when cate_id='fin_18' then cosin_similarity else -1 end) as fin_18_trend
    ,max(case when cate_id='fin_19' then cosin_similarity else -1 end) as fin_19_trend
    ,max(case when cate_id='fin_2' then cosin_similarity else -1 end) as fin_2_trend
    ,max(case when cate_id='fin_20' then cosin_similarity else -1 end) as fin_20_trend
    ,max(case when cate_id='fin_21' then cosin_similarity else -1 end) as fin_21_trend
    ,max(case when cate_id='fin_22' then cosin_similarity else -1 end) as fin_22_trend
    ,max(case when cate_id='fin_23' then cosin_similarity else -1 end) as fin_23_trend
    ,max(case when cate_id='fin_24' then cosin_similarity else -1 end) as fin_24_trend
    ,max(case when cate_id='fin_25' then cosin_similarity else -1 end) as fin_25_trend
    ,max(case when cate_id='fin_26' then cosin_similarity else -1 end) as fin_26_trend
    ,max(case when cate_id='fin_27' then cosin_similarity else -1 end) as fin_27_trend
    ,max(case when cate_id='fin_28' then cosin_similarity else -1 end) as fin_28_trend
    ,max(case when cate_id='fin_29' then cosin_similarity else -1 end) as fin_29_trend
    ,max(case when cate_id='fin_3' then cosin_similarity else -1 end) as fin_3_trend
    ,max(case when cate_id='fin_30' then cosin_similarity else -1 end) as fin_30_trend
    ,max(case when cate_id='fin_31' then cosin_similarity else -1 end) as fin_31_trend
    ,max(case when cate_id='fin_32' then cosin_similarity else -1 end) as fin_32_trend
    ,max(case when cate_id='fin_33' then cosin_similarity else -1 end) as fin_33_trend
    ,max(case when cate_id='fin_34' then cosin_similarity else -1 end) as fin_34_trend
    ,max(case when cate_id='fin_35' then cosin_similarity else -1 end) as fin_35_trend
    ,max(case when cate_id='fin_36' then cosin_similarity else -1 end) as fin_36_trend
    ,max(case when cate_id='fin_37' then cosin_similarity else -1 end) as fin_37_trend
    ,max(case when cate_id='fin_38' then cosin_similarity else -1 end) as fin_38_trend
    ,max(case when cate_id='fin_39' then cosin_similarity else -1 end) as fin_39_trend
    ,max(case when cate_id='fin_4' then cosin_similarity else -1 end) as fin_4_trend
    ,max(case when cate_id='fin_40' then cosin_similarity else -1 end) as fin_40_trend
    ,max(case when cate_id='fin_41' then cosin_similarity else -1 end) as fin_41_trend
    ,max(case when cate_id='fin_42' then cosin_similarity else -1 end) as fin_42_trend
    ,max(case when cate_id='fin_43' then cosin_similarity else -1 end) as fin_43_trend
    ,max(case when cate_id='fin_44' then cosin_similarity else -1 end) as fin_44_trend
    ,max(case when cate_id='fin_45' then cosin_similarity else -1 end) as fin_45_trend
    ,max(case when cate_id='fin_46' then cosin_similarity else -1 end) as fin_46_trend
    ,max(case when cate_id='fin_47' then cosin_similarity else -1 end) as fin_47_trend
    ,max(case when cate_id='fin_48' then cosin_similarity else -1 end) as fin_48_trend
    ,max(case when cate_id='fin_49' then cosin_similarity else -1 end) as fin_49_trend
    ,max(case when cate_id='fin_5' then cosin_similarity else -1 end) as fin_5_trend
    ,max(case when cate_id='fin_50' then cosin_similarity else -1 end) as fin_50_trend
    ,max(case when cate_id='fin_51' then cosin_similarity else -1 end) as fin_51_trend
    ,max(case when cate_id='fin_52' then cosin_similarity else -1 end) as fin_52_trend
    ,max(case when cate_id='fin_6' then cosin_similarity else -1 end) as fin_6_trend
    ,max(case when cate_id='fin_7' then cosin_similarity else -1 end) as fin_7_trend
    ,max(case when cate_id='fin_8' then cosin_similarity else -1 end) as fin_8_trend
    ,max(case when cate_id='fin_9' then cosin_similarity else -1 end) as fin_9_trend
    ,max(case when cate_id='tgi1_1_0' then cosin_similarity else -1 end) as tgi1_1_0_trend
    ,max(case when cate_id='tgi1_1_1' then cosin_similarity else -1 end) as tgi1_1_1_trend
    ,max(case when cate_id='tgi1_1_2' then cosin_similarity else -1 end) as tgi1_1_2_trend
    ,max(case when cate_id='tgi1_1_3' then cosin_similarity else -1 end) as tgi1_1_3_trend
    ,max(case when cate_id='tgi1_2_0' then cosin_similarity else -1 end) as tgi1_2_0_trend
    ,max(case when cate_id='tgi1_2_1' then cosin_similarity else -1 end) as tgi1_2_1_trend
    ,max(case when cate_id='tgi1_2_2' then cosin_similarity else -1 end) as tgi1_2_2_trend
    ,max(case when cate_id='tgi1_2_3' then cosin_similarity else -1 end) as tgi1_2_3_trend
    ,max(case when cate_id='tgi1_3_0' then cosin_similarity else -1 end) as tgi1_3_0_trend
    ,max(case when cate_id='tgi1_3_1' then cosin_similarity else -1 end) as tgi1_3_1_trend
    ,max(case when cate_id='tgi1_3_2' then cosin_similarity else -1 end) as tgi1_3_2_trend
    ,max(case when cate_id='tgi1_4_0' then cosin_similarity else -1 end) as tgi1_4_0_trend
    ,max(case when cate_id='tgi1_4_1' then cosin_similarity else -1 end) as tgi1_4_1_trend
    ,max(case when cate_id='tgi1_4_2' then cosin_similarity else -1 end) as tgi1_4_2_trend
    ,max(case when cate_id='tgi1_5_0' then cosin_similarity else -1 end) as tgi1_5_0_trend
    ,max(case when cate_id='tgi1_5_1' then cosin_similarity else -1 end) as tgi1_5_1_trend
    ,max(case when cate_id='tgi1_5_2' then cosin_similarity else -1 end) as tgi1_5_2_trend
    ,max(case when cate_id='tgi1_5_3' then cosin_similarity else -1 end) as tgi1_5_3_trend
from $income_new_uninstall_avg_embedding_cosin_tmp
where day=${day}
group by device
),

income_temp_ratio_features_12m as (
select
    device
   ,round(COALESCE( A13_12m_cnt*1.000000/total_12m_cnt,0),6) as A13_12m_ratio
   ,round(COALESCE( A13_1_12m_cnt*1.000000/total_12m_cnt,0),6) as A13_1_12m_ratio
   ,round(COALESCE( A13_2_12m_cnt*1.000000/total_12m_cnt,0),6) as A13_2_12m_ratio
   ,round(COALESCE( A13_3_12m_cnt*1.000000/total_12m_cnt,0),6) as A13_3_12m_ratio
   ,round(COALESCE( A13_4_12m_cnt*1.000000/total_12m_cnt,0),6) as A13_4_12m_ratio
   ,round(COALESCE( A20_1_12m_cnt*1.000000/total_12m_cnt,0),6) as A20_1_12m_ratio
   ,round(COALESCE( A20_4_12m_cnt*1.000000/total_12m_cnt,0),6) as A20_4_12m_ratio
   ,round(COALESCE( A20_7_12m_cnt*1.000000/total_12m_cnt,0),6) as A20_7_12m_ratio
   ,round(COALESCE( A20_9_12m_cnt*1.000000/total_12m_cnt,0),6) as A20_9_12m_ratio
   ,round(COALESCE( A4_12m_cnt*1.000000/total_12m_cnt,0),6) as A4_12m_ratio
   ,round(COALESCE( A4_1_12m_cnt*1.000000/total_12m_cnt,0),6) as A4_1_12m_ratio
   ,round(COALESCE( A4_2_12m_cnt*1.000000/total_12m_cnt,0),6) as A4_2_12m_ratio
   ,round(COALESCE( A4_3_12m_cnt*1.000000/total_12m_cnt,0),6) as A4_3_12m_ratio
   ,round(COALESCE( A5_12m_cnt*1.000000/total_12m_cnt,0),6) as  A5_12m_ratio
   ,round(COALESCE( A5_1_12m_cnt*1.000000/total_12m_cnt,0),6) as  A5_1_12m_ratio
   ,round(COALESCE( A5_2_12m_cnt*1.000000/total_12m_cnt,0),6) as  A5_2_12m_ratio
   ,round(COALESCE( A5_3_12m_cnt*1.000000/total_12m_cnt,0),6) as  A5_3_12m_ratio
   ,round(COALESCE( A6_12m_cnt*1.000000/total_12m_cnt,0),6) as  A6_12m_ratio
   ,round(COALESCE( A6_1_12m_cnt*1.000000/total_12m_cnt,0),6) as  A6_1_12m_ratio
   ,round(COALESCE( A6_2_12m_cnt*1.000000/total_12m_cnt,0),6) as  A6_2_12m_ratio
   ,round(COALESCE( A6_3_12m_cnt*1.000000/total_12m_cnt,0),6) as  A6_3_12m_ratio
   ,round(COALESCE( A9_12m_cnt*1.000000/total_12m_cnt,0),6) as  A9_12m_ratio
   ,round(COALESCE( A9_1_12m_cnt*1.000000/total_12m_cnt,0),6) as  A9_1_12m_ratio
   ,round(COALESCE( A9_2_12m_cnt*1.000000/total_12m_cnt,0),6) as  A9_2_12m_ratio
   ,round(COALESCE( A9_3_12m_cnt*1.000000/total_12m_cnt,0),6) as  A9_3_12m_ratio
   ,round(COALESCE( A9_4_12m_cnt*1.000000/total_12m_cnt,0),6) as  A9_4_12m_ratio
   ,round(COALESCE( B6_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_12m_ratio
   ,round(COALESCE( B6_1_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_1_12m_ratio
   ,round(COALESCE( B6_2_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_2_12m_ratio
   ,round(COALESCE( B6_3_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_3_12m_ratio
   ,round(COALESCE( B6_4_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_4_12m_ratio
   ,round(COALESCE( B6_5_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_5_12m_ratio
   ,round(COALESCE( B6_6_12m_cnt*1.000000/total_12m_cnt,0),6) as  B6_6_12m_ratio
   ,round(COALESCE( B7_12m_cnt*1.000000/total_12m_cnt,0),6) as  B7_12m_ratio
   ,round(COALESCE( B7_1_12m_cnt*1.000000/total_12m_cnt,0),6) as  B7_1_12m_ratio
   ,round(COALESCE( B7_2_12m_cnt*1.000000/total_12m_cnt,0),6) as  B7_2_12m_ratio
   ,round(COALESCE( B8_12m_cnt*1.000000/total_12m_cnt,0),6) as  B8_12m_ratio
   ,round(COALESCE( B8_1_12m_cnt*1.000000/total_12m_cnt,0),6) as  B8_1_12m_ratio
   ,round(COALESCE( B8_2_12m_cnt*1.000000/total_12m_cnt,0),6) as  B8_2_12m_ratio
   ,round(COALESCE( B8_3_12m_cnt*1.000000/total_12m_cnt,0),6) as  B8_3_12m_ratio
   ,round(COALESCE( fin_13_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_13_12m_ratio
   ,round(COALESCE( fin_19_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_19_12m_ratio
   ,round(COALESCE( fin_24_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_24_12m_ratio
   ,round(COALESCE( fin_25_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_25_12m_ratio
   ,round(COALESCE( fin_26_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_26_12m_ratio
   ,round(COALESCE( fin_27_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_27_12m_ratio
   ,round(COALESCE( fin_28_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_28_12m_ratio
   ,round(COALESCE( fin_29_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_29_12m_ratio
   ,round(COALESCE( fin_3_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_3_12m_ratio
   ,round(COALESCE( fin_30_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_30_12m_ratio
   ,round(COALESCE( fin_31_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_31_12m_ratio
   ,round(COALESCE( fin_32_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_32_12m_ratio
   ,round(COALESCE( fin_4_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_4_12m_ratio
   ,round(COALESCE( fin_43_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_43_12m_ratio
   ,round(COALESCE( fin_46_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_46_12m_ratio
   ,round(COALESCE( fin_47_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_47_12m_ratio
   ,round(COALESCE( fin_48_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_48_12m_ratio
   ,round(COALESCE( fin_49_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_49_12m_ratio
   ,round(COALESCE( fin_5_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_5_12m_ratio
   ,round(COALESCE( fin_50_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_50_12m_ratio
   ,round(COALESCE( fin_51_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_51_12m_ratio
   ,round(COALESCE( fin_52_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_52_12m_ratio
   ,round(COALESCE( fin_6_12m_cnt*1.000000/total_12m_cnt,0),6) as  fin_6_12m_ratio
    from(
    select
        device
        ,count(pkg) as total_12m_cnt
        ,count(case when cate_id='A13' then pkg else null end ) as A13_12m_cnt
        ,count(case when cate_id='A13_1' then pkg else null end ) as A13_1_12m_cnt
        ,count(case when cate_id='A13_2' then pkg else null end ) as A13_2_12m_cnt
        ,count(case when cate_id='A13_3' then pkg else null end ) as A13_3_12m_cnt
        ,count(case when cate_id='A13_4' then pkg else null end ) as A13_4_12m_cnt
        ,count(case when cate_id='A20_1' then pkg else null end ) as A20_1_12m_cnt
        ,count(case when cate_id='A20_4' then pkg else null end ) as A20_4_12m_cnt
        ,count(case when cate_id='A20_7' then pkg else null end ) as A20_7_12m_cnt
        ,count(case when cate_id='A20_9' then pkg else null end ) as A20_9_12m_cnt
        ,count(case when cate_id='A4' then pkg else null end ) as A4_12m_cnt
        ,count(case when cate_id='A4_1' then pkg else null end ) as A4_1_12m_cnt
        ,count(case when cate_id='A4_2' then pkg else null end ) as A4_2_12m_cnt
        ,count(case when cate_id='A4_3' then pkg else null end ) as A4_3_12m_cnt
        ,count(case when cate_id='A5' then pkg else null end ) as A5_12m_cnt
        ,count(case when cate_id='A5_1' then pkg else null end ) as A5_1_12m_cnt
        ,count(case when cate_id='A5_2' then pkg else null end ) as A5_2_12m_cnt
        ,count(case when cate_id='A5_3' then pkg else null end ) as A5_3_12m_cnt
        ,count(case when cate_id='A6' then pkg else null end ) as A6_12m_cnt
        ,count(case when cate_id='A6_1' then pkg else null end ) as A6_1_12m_cnt
        ,count(case when cate_id='A6_2' then pkg else null end ) as A6_2_12m_cnt
        ,count(case when cate_id='A6_3' then pkg else null end ) as A6_3_12m_cnt
        ,count(case when cate_id='A9' then pkg else null end ) as A9_12m_cnt
        ,count(case when cate_id='A9_1' then pkg else null end ) as A9_1_12m_cnt
        ,count(case when cate_id='A9_2' then pkg else null end ) as A9_2_12m_cnt
        ,count(case when cate_id='A9_3' then pkg else null end ) as A9_3_12m_cnt
        ,count(case when cate_id='A9_4' then pkg else null end ) as A9_4_12m_cnt
        ,count(case when cate_id='B6' then pkg else null end ) as B6_12m_cnt
        ,count(case when cate_id='B6_1' then pkg else null end ) as B6_1_12m_cnt
        ,count(case when cate_id='B6_2' then pkg else null end ) as B6_2_12m_cnt
        ,count(case when cate_id='B6_3' then pkg else null end ) as B6_3_12m_cnt
        ,count(case when cate_id='B6_4' then pkg else null end ) as B6_4_12m_cnt
        ,count(case when cate_id='B6_5' then pkg else null end ) as B6_5_12m_cnt
        ,count(case when cate_id='B6_6' then pkg else null end ) as B6_6_12m_cnt
        ,count(case when cate_id='B7' then pkg else null end ) as B7_12m_cnt
        ,count(case when cate_id='B7_1' then pkg else null end ) as B7_1_12m_cnt
        ,count(case when cate_id='B7_2' then pkg else null end ) as B7_2_12m_cnt
        ,count(case when cate_id='B8' then pkg else null end ) as B8_12m_cnt
        ,count(case when cate_id='B8_1' then pkg else null end ) as B8_1_12m_cnt
        ,count(case when cate_id='B8_2' then pkg else null end ) as B8_2_12m_cnt
        ,count(case when cate_id='B8_3' then pkg else null end ) as B8_3_12m_cnt
        ,count(case when cate_id='fin_13' then pkg else null end ) as fin_13_12m_cnt
        ,count(case when cate_id='fin_19' then pkg else null end ) as fin_19_12m_cnt
        ,count(case when cate_id='fin_24' then pkg else null end ) as fin_24_12m_cnt
        ,count(case when cate_id='fin_25' then pkg else null end ) as fin_25_12m_cnt
        ,count(case when cate_id='fin_26' then pkg else null end ) as fin_26_12m_cnt
        ,count(case when cate_id='fin_27' then pkg else null end ) as fin_27_12m_cnt
        ,count(case when cate_id='fin_28' then pkg else null end ) as fin_28_12m_cnt
        ,count(case when cate_id='fin_29' then pkg else null end ) as fin_29_12m_cnt
        ,count(case when cate_id='fin_3' then pkg else null end ) as fin_3_12m_cnt
        ,count(case when cate_id='fin_30' then pkg else null end ) as fin_30_12m_cnt
        ,count(case when cate_id='fin_31' then pkg else null end ) as fin_31_12m_cnt
        ,count(case when cate_id='fin_32' then pkg else null end ) as fin_32_12m_cnt
        ,count(case when cate_id='fin_4' then pkg else null end ) as fin_4_12m_cnt
        ,count(case when cate_id='fin_43' then pkg else null end ) as fin_43_12m_cnt
        ,count(case when cate_id='fin_46' then pkg else null end ) as fin_46_12m_cnt
        ,count(case when cate_id='fin_47' then pkg else null end ) as fin_47_12m_cnt
        ,count(case when cate_id='fin_48' then pkg else null end ) as fin_48_12m_cnt
        ,count(case when cate_id='fin_49' then pkg else null end ) as fin_49_12m_cnt
        ,count(case when cate_id='fin_5' then pkg else null end ) as fin_5_12m_cnt
        ,count(case when cate_id='fin_50' then pkg else null end ) as fin_50_12m_cnt
        ,count(case when cate_id='fin_51' then pkg else null end ) as fin_51_12m_cnt
        ,count(case when cate_id='fin_52' then pkg else null end ) as fin_52_12m_cnt
        ,count(case when cate_id='fin_6' then pkg else null end ) as fin_6_12m_cnt
    from raw_table where datediff(date,flag_day)<=365 group by device
   )t2
),
income_temp_ratio_features_6m as (
select
    device
   ,round(COALESCE( A13_6m_cnt*1.000000/total_6m_cnt,0),6) as  A13_6m_ratio
   ,round(COALESCE( A13_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A13_1_6m_ratio
   ,round(COALESCE( A13_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  A13_2_6m_ratio
   ,round(COALESCE( A13_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  A13_3_6m_ratio
   ,round(COALESCE( A13_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  A13_4_6m_ratio
   ,round(COALESCE( A14_6m_cnt*1.000000/total_6m_cnt,0),6) as  A14_6m_ratio
   ,round(COALESCE( A18_6m_cnt*1.000000/total_6m_cnt,0),6) as  A18_6m_ratio
   ,round(COALESCE( A20_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_6m_ratio
   ,round(COALESCE( A20_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_1_6m_ratio
   ,round(COALESCE( A20_10_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_10_6m_ratio
   ,round(COALESCE( A20_11_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_11_6m_ratio
   ,round(COALESCE( A20_12_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_12_6m_ratio
   ,round(COALESCE( A20_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_2_6m_ratio
   ,round(COALESCE( A20_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_4_6m_ratio
   ,round(COALESCE( A20_5_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_5_6m_ratio
   ,round(COALESCE( A20_6_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_6_6m_ratio
   ,round(COALESCE( A20_7_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_7_6m_ratio
   ,round(COALESCE( A20_8_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_8_6m_ratio
   ,round(COALESCE( A20_9_6m_cnt*1.000000/total_6m_cnt,0),6) as  A20_9_6m_ratio
   ,round(COALESCE( A2_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A2_1_6m_ratio
   ,round(COALESCE( A2_6_6m_cnt*1.000000/total_6m_cnt,0),6) as  A2_6_6m_ratio
   ,round(COALESCE( A4_6m_cnt*1.000000/total_6m_cnt,0),6) as  A4_6m_ratio
   ,round(COALESCE( A4_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A4_1_6m_ratio
   ,round(COALESCE( A4_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  A4_2_6m_ratio
   ,round(COALESCE( A4_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  A4_3_6m_ratio
   ,round(COALESCE( A5_6m_cnt*1.000000/total_6m_cnt,0),6) as  A5_6m_ratio
   ,round(COALESCE( A5_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A5_1_6m_ratio
   ,round(COALESCE( A5_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  A5_2_6m_ratio
   ,round(COALESCE( A5_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  A5_3_6m_ratio
   ,round(COALESCE( A6_6m_cnt*1.000000/total_6m_cnt,0),6) as  A6_6m_ratio
   ,round(COALESCE( A6_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A6_1_6m_ratio
   ,round(COALESCE( A6_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  A6_2_6m_ratio
   ,round(COALESCE( A6_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  A6_3_6m_ratio
   ,round(COALESCE( A7_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A7_1_6m_ratio
   ,round(COALESCE( A7_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  A7_4_6m_ratio
   ,round(COALESCE( A8_6m_cnt*1.000000/total_6m_cnt,0),6) as  A8_6m_ratio
   ,round(COALESCE( A8_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  A8_4_6m_ratio
   ,round(COALESCE( A8_6_6m_cnt*1.000000/total_6m_cnt,0),6) as  A8_6_6m_ratio
   ,round(COALESCE( A9_6m_cnt*1.000000/total_6m_cnt,0),6) as  A9_6m_ratio
   ,round(COALESCE( A9_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  A9_1_6m_ratio
   ,round(COALESCE( A9_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  A9_2_6m_ratio
   ,round(COALESCE( A9_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  A9_3_6m_ratio
   ,round(COALESCE( A9_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  A9_4_6m_ratio
   ,round(COALESCE( B11_6m_cnt*1.000000/total_6m_cnt,0),6) as  B11_6m_ratio
   ,round(COALESCE( B12_6m_cnt*1.000000/total_6m_cnt,0),6) as  B12_6m_ratio
   ,round(COALESCE( B6_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_6m_ratio
   ,round(COALESCE( B6_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_1_6m_ratio
   ,round(COALESCE( B6_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_2_6m_ratio
   ,round(COALESCE( B6_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_3_6m_ratio
   ,round(COALESCE( B6_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_4_6m_ratio
   ,round(COALESCE( B6_5_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_5_6m_ratio
   ,round(COALESCE( B6_6_6m_cnt*1.000000/total_6m_cnt,0),6) as  B6_6_6m_ratio
   ,round(COALESCE( B7_6m_cnt*1.000000/total_6m_cnt,0),6) as  B7_6m_ratio
   ,round(COALESCE( B7_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  B7_1_6m_ratio
   ,round(COALESCE( B7_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  B7_2_6m_ratio
   ,round(COALESCE( B8_6m_cnt*1.000000/total_6m_cnt,0),6) as  B8_6m_ratio
   ,round(COALESCE( B8_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  B8_1_6m_ratio
   ,round(COALESCE( B8_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  B8_2_6m_ratio
   ,round(COALESCE( B8_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  B8_3_6m_ratio
   ,round(COALESCE( fin_13_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_13_6m_ratio
   ,round(COALESCE( fin_15_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_15_6m_ratio
   ,round(COALESCE( fin_19_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_19_6m_ratio
   ,round(COALESCE( fin_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_2_6m_ratio
   ,round(COALESCE( fin_22_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_22_6m_ratio
   ,round(COALESCE( fin_24_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_24_6m_ratio
   ,round(COALESCE( fin_25_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_25_6m_ratio
   ,round(COALESCE( fin_26_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_26_6m_ratio
   ,round(COALESCE( fin_27_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_27_6m_ratio
   ,round(COALESCE( fin_28_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_28_6m_ratio
   ,round(COALESCE( fin_29_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_29_6m_ratio
   ,round(COALESCE( fin_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_3_6m_ratio
   ,round(COALESCE( fin_30_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_30_6m_ratio
   ,round(COALESCE( fin_31_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_31_6m_ratio
   ,round(COALESCE( fin_32_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_32_6m_ratio
   ,round(COALESCE( fin_36_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_36_6m_ratio
   ,round(COALESCE( fin_39_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_39_6m_ratio
   ,round(COALESCE( fin_4_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_4_6m_ratio
   ,round(COALESCE( fin_40_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_40_6m_ratio
   ,round(COALESCE( fin_43_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_43_6m_ratio
   ,round(COALESCE( fin_46_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_46_6m_ratio
   ,round(COALESCE( fin_47_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_47_6m_ratio
   ,round(COALESCE( fin_48_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_48_6m_ratio
   ,round(COALESCE( fin_49_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_49_6m_ratio
   ,round(COALESCE( fin_5_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_5_6m_ratio
   ,round(COALESCE( fin_50_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_50_6m_ratio
   ,round(COALESCE( fin_51_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_51_6m_ratio
   ,round(COALESCE( fin_52_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_52_6m_ratio
   ,round(COALESCE( fin_6_6m_cnt*1.000000/total_6m_cnt,0),6) as  fin_6_6m_ratio
   ,round(COALESCE( tgi1_2_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  tgi1_2_3_6m_ratio
   ,round(COALESCE( tgi1_5_0_6m_cnt*1.000000/total_6m_cnt,0),6) as  tgi1_5_0_6m_ratio
   ,round(COALESCE( tgi1_5_1_6m_cnt*1.000000/total_6m_cnt,0),6) as  tgi1_5_1_6m_ratio
   ,round(COALESCE( tgi1_5_2_6m_cnt*1.000000/total_6m_cnt,0),6) as  tgi1_5_2_6m_ratio
   ,round(COALESCE( tgi1_5_3_6m_cnt*1.000000/total_6m_cnt,0),6) as  tgi1_5_3_6m_ratio
    from(
    select device
        ,count(pkg) as total_6m_cnt
        ,count(case when cate_id='A13' then pkg else null end ) as A13_6m_cnt
        ,count(case when cate_id='A13_1' then pkg else null end ) as A13_1_6m_cnt
        ,count(case when cate_id='A13_2' then pkg else null end ) as A13_2_6m_cnt
        ,count(case when cate_id='A13_3' then pkg else null end ) as A13_3_6m_cnt
        ,count(case when cate_id='A13_4' then pkg else null end ) as A13_4_6m_cnt
        ,count(case when cate_id='A14' then pkg else null end ) as A14_6m_cnt
        ,count(case when cate_id='A18' then pkg else null end ) as A18_6m_cnt
        ,count(case when cate_id='A20' then pkg else null end ) as A20_6m_cnt
        ,count(case when cate_id='A20_1' then pkg else null end ) as A20_1_6m_cnt
        ,count(case when cate_id='A20_10' then pkg else null end ) as A20_10_6m_cnt
        ,count(case when cate_id='A20_11' then pkg else null end ) as A20_11_6m_cnt
        ,count(case when cate_id='A20_12' then pkg else null end ) as A20_12_6m_cnt
        ,count(case when cate_id='A20_2' then pkg else null end ) as A20_2_6m_cnt
        ,count(case when cate_id='A20_4' then pkg else null end ) as A20_4_6m_cnt
        ,count(case when cate_id='A20_5' then pkg else null end ) as A20_5_6m_cnt
        ,count(case when cate_id='A20_6' then pkg else null end ) as A20_6_6m_cnt
        ,count(case when cate_id='A20_7' then pkg else null end ) as A20_7_6m_cnt
        ,count(case when cate_id='A20_8' then pkg else null end ) as A20_8_6m_cnt
        ,count(case when cate_id='A20_9' then pkg else null end ) as A20_9_6m_cnt
        ,count(case when cate_id='A2_1' then pkg else null end ) as A2_1_6m_cnt
        ,count(case when cate_id='A2_6' then pkg else null end ) as A2_6_6m_cnt
        ,count(case when cate_id='A4' then pkg else null end ) as A4_6m_cnt
        ,count(case when cate_id='A4_1' then pkg else null end ) as A4_1_6m_cnt
        ,count(case when cate_id='A4_2' then pkg else null end ) as A4_2_6m_cnt
        ,count(case when cate_id='A4_3' then pkg else null end ) as A4_3_6m_cnt
        ,count(case when cate_id='A5' then pkg else null end ) as A5_6m_cnt
        ,count(case when cate_id='A5_1' then pkg else null end ) as A5_1_6m_cnt
        ,count(case when cate_id='A5_2' then pkg else null end ) as A5_2_6m_cnt
        ,count(case when cate_id='A5_3' then pkg else null end ) as A5_3_6m_cnt
        ,count(case when cate_id='A6' then pkg else null end ) as A6_6m_cnt
        ,count(case when cate_id='A6_1' then pkg else null end ) as A6_1_6m_cnt
        ,count(case when cate_id='A6_2' then pkg else null end ) as A6_2_6m_cnt
        ,count(case when cate_id='A6_3' then pkg else null end ) as A6_3_6m_cnt
        ,count(case when cate_id='A7_1' then pkg else null end ) as A7_1_6m_cnt
        ,count(case when cate_id='A7_4' then pkg else null end ) as A7_4_6m_cnt
        ,count(case when cate_id='A8' then pkg else null end ) as A8_6m_cnt
        ,count(case when cate_id='A8_4' then pkg else null end ) as A8_4_6m_cnt
        ,count(case when cate_id='A8_6' then pkg else null end ) as A8_6_6m_cnt
        ,count(case when cate_id='A9' then pkg else null end ) as A9_6m_cnt
        ,count(case when cate_id='A9_1' then pkg else null end ) as A9_1_6m_cnt
        ,count(case when cate_id='A9_2' then pkg else null end ) as A9_2_6m_cnt
        ,count(case when cate_id='A9_3' then pkg else null end ) as A9_3_6m_cnt
        ,count(case when cate_id='A9_4' then pkg else null end ) as A9_4_6m_cnt
        ,count(case when cate_id='B11' then pkg else null end ) as B11_6m_cnt
        ,count(case when cate_id='B12' then pkg else null end ) as B12_6m_cnt
        ,count(case when cate_id='B6' then pkg else null end ) as B6_6m_cnt
        ,count(case when cate_id='B6_1' then pkg else null end ) as B6_1_6m_cnt
        ,count(case when cate_id='B6_2' then pkg else null end ) as B6_2_6m_cnt
        ,count(case when cate_id='B6_3' then pkg else null end ) as B6_3_6m_cnt
        ,count(case when cate_id='B6_4' then pkg else null end ) as B6_4_6m_cnt
        ,count(case when cate_id='B6_5' then pkg else null end ) as B6_5_6m_cnt
        ,count(case when cate_id='B6_6' then pkg else null end ) as B6_6_6m_cnt
        ,count(case when cate_id='B7' then pkg else null end ) as B7_6m_cnt
        ,count(case when cate_id='B7_1' then pkg else null end ) as B7_1_6m_cnt
        ,count(case when cate_id='B7_2' then pkg else null end ) as B7_2_6m_cnt
        ,count(case when cate_id='B8' then pkg else null end ) as B8_6m_cnt
        ,count(case when cate_id='B8_1' then pkg else null end ) as B8_1_6m_cnt
        ,count(case when cate_id='B8_2' then pkg else null end ) as B8_2_6m_cnt
        ,count(case when cate_id='B8_3' then pkg else null end ) as B8_3_6m_cnt
        ,count(case when cate_id='fin_13' then pkg else null end ) as fin_13_6m_cnt
        ,count(case when cate_id='fin_15' then pkg else null end ) as fin_15_6m_cnt
        ,count(case when cate_id='fin_19' then pkg else null end ) as fin_19_6m_cnt
        ,count(case when cate_id='fin_2' then pkg else null end ) as fin_2_6m_cnt
        ,count(case when cate_id='fin_22' then pkg else null end ) as fin_22_6m_cnt
        ,count(case when cate_id='fin_24' then pkg else null end ) as fin_24_6m_cnt
        ,count(case when cate_id='fin_25' then pkg else null end ) as fin_25_6m_cnt
        ,count(case when cate_id='fin_26' then pkg else null end ) as fin_26_6m_cnt
        ,count(case when cate_id='fin_27' then pkg else null end ) as fin_27_6m_cnt
        ,count(case when cate_id='fin_28' then pkg else null end ) as fin_28_6m_cnt
        ,count(case when cate_id='fin_29' then pkg else null end ) as fin_29_6m_cnt
        ,count(case when cate_id='fin_3' then pkg else null end ) as fin_3_6m_cnt
        ,count(case when cate_id='fin_30' then pkg else null end ) as fin_30_6m_cnt
        ,count(case when cate_id='fin_31' then pkg else null end ) as fin_31_6m_cnt
        ,count(case when cate_id='fin_32' then pkg else null end ) as fin_32_6m_cnt
        ,count(case when cate_id='fin_36' then pkg else null end ) as fin_36_6m_cnt
        ,count(case when cate_id='fin_39' then pkg else null end ) as fin_39_6m_cnt
        ,count(case when cate_id='fin_4' then pkg else null end ) as fin_4_6m_cnt
        ,count(case when cate_id='fin_40' then pkg else null end ) as fin_40_6m_cnt
        ,count(case when cate_id='fin_43' then pkg else null end ) as fin_43_6m_cnt
        ,count(case when cate_id='fin_46' then pkg else null end ) as fin_46_6m_cnt
        ,count(case when cate_id='fin_47' then pkg else null end ) as fin_47_6m_cnt
        ,count(case when cate_id='fin_48' then pkg else null end ) as fin_48_6m_cnt
        ,count(case when cate_id='fin_49' then pkg else null end ) as fin_49_6m_cnt
        ,count(case when cate_id='fin_5' then pkg else null end ) as fin_5_6m_cnt
        ,count(case when cate_id='fin_50' then pkg else null end ) as fin_50_6m_cnt
        ,count(case when cate_id='fin_51' then pkg else null end ) as fin_51_6m_cnt
        ,count(case when cate_id='fin_52' then pkg else null end ) as fin_52_6m_cnt
        ,count(case when cate_id='fin_6' then pkg else null end ) as fin_6_6m_cnt
        ,count(case when cate_id='tgi1_2_3' then pkg else null end ) as tgi1_2_3_6m_cnt
        ,count(case when cate_id='tgi1_5_0' then pkg else null end ) as tgi1_5_0_6m_cnt
        ,count(case when cate_id='tgi1_5_1' then pkg else null end ) as tgi1_5_1_6m_cnt
        ,count(case when cate_id='tgi1_5_2' then pkg else null end ) as tgi1_5_2_6m_cnt
        ,count(case when cate_id='tgi1_5_3' then pkg else null end ) as tgi1_5_3_6m_cnt
    from raw_table where datediff(date,flag_day)<=180 group by device
    )t2
),
income_temp_ratio_features_3m as (
select
    device
   ,round(COALESCE( A13_3m_cnt*1.000000/total_3m_cnt,0),6) as  A13_3m_ratio
   ,round(COALESCE( A13_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  A13_1_3m_ratio
   ,round(COALESCE( A13_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  A13_2_3m_ratio
   ,round(COALESCE( A13_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  A13_3_3m_ratio
   ,round(COALESCE( A13_4_3m_cnt*1.000000/total_3m_cnt,0),6) as  A13_4_3m_ratio
   ,round(COALESCE( A20_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_3m_ratio
   ,round(COALESCE( A20_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_1_3m_ratio
   ,round(COALESCE( A20_10_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_10_3m_ratio
   ,round(COALESCE( A20_11_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_11_3m_ratio
   ,round(COALESCE( A20_12_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_12_3m_ratio
   ,round(COALESCE( A20_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_2_3m_ratio
   ,round(COALESCE( A20_4_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_4_3m_ratio
   ,round(COALESCE( A20_5_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_5_3m_ratio
   ,round(COALESCE( A20_6_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_6_3m_ratio
   ,round(COALESCE( A20_8_3m_cnt*1.000000/total_3m_cnt,0),6) as  A20_8_3m_ratio
   ,round(COALESCE( A4_3m_cnt*1.000000/total_3m_cnt,0),6) as  A4_3m_ratio
   ,round(COALESCE( A4_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  A4_1_3m_ratio
   ,round(COALESCE( A4_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  A4_2_3m_ratio
   ,round(COALESCE( A4_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  A4_3_3m_ratio
   ,round(COALESCE( A5_3m_cnt*1.000000/total_3m_cnt,0),6) as  A5_3m_ratio
   ,round(COALESCE( A5_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  A5_1_3m_ratio
   ,round(COALESCE( A5_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  A5_2_3m_ratio
   ,round(COALESCE( A5_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  A5_3_3m_ratio
   ,round(COALESCE( A6_3m_cnt*1.000000/total_3m_cnt,0),6) as  A6_3m_ratio
   ,round(COALESCE( A6_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  A6_1_3m_ratio
   ,round(COALESCE( A6_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  A6_2_3m_ratio
   ,round(COALESCE( A6_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  A6_3_3m_ratio
   ,round(COALESCE( A9_3m_cnt*1.000000/total_3m_cnt,0),6) as  A9_3m_ratio
   ,round(COALESCE( A9_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  A9_1_3m_ratio
   ,round(COALESCE( A9_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  A9_2_3m_ratio
   ,round(COALESCE( A9_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  A9_3_3m_ratio
   ,round(COALESCE( A9_4_3m_cnt*1.000000/total_3m_cnt,0),6) as  A9_4_3m_ratio
   ,round(COALESCE( B6_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_3m_ratio
   ,round(COALESCE( B6_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_1_3m_ratio
   ,round(COALESCE( B6_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_2_3m_ratio
   ,round(COALESCE( B6_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_3_3m_ratio
   ,round(COALESCE( B6_4_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_4_3m_ratio
   ,round(COALESCE( B6_5_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_5_3m_ratio
   ,round(COALESCE( B6_6_3m_cnt*1.000000/total_3m_cnt,0),6) as  B6_6_3m_ratio
   ,round(COALESCE( B7_3m_cnt*1.000000/total_3m_cnt,0),6) as  B7_3m_ratio
   ,round(COALESCE( B7_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  B7_1_3m_ratio
   ,round(COALESCE( B7_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  B7_2_3m_ratio
   ,round(COALESCE( B8_3m_cnt*1.000000/total_3m_cnt,0),6) as  B8_3m_ratio
   ,round(COALESCE( B8_1_3m_cnt*1.000000/total_3m_cnt,0),6) as  B8_1_3m_ratio
   ,round(COALESCE( B8_2_3m_cnt*1.000000/total_3m_cnt,0),6) as  B8_2_3m_ratio
   ,round(COALESCE( B8_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  B8_3_3m_ratio
   ,round(COALESCE( fin_12_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_12_3m_ratio
   ,round(COALESCE( fin_24_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_24_3m_ratio
   ,round(COALESCE( fin_25_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_25_3m_ratio
   ,round(COALESCE( fin_26_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_26_3m_ratio
   ,round(COALESCE( fin_27_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_27_3m_ratio
   ,round(COALESCE( fin_28_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_28_3m_ratio
   ,round(COALESCE( fin_29_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_29_3m_ratio
   ,round(COALESCE( fin_3_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_3_3m_ratio
   ,round(COALESCE( fin_30_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_30_3m_ratio
   ,round(COALESCE( fin_31_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_31_3m_ratio
   ,round(COALESCE( fin_32_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_32_3m_ratio
   ,round(COALESCE( fin_4_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_4_3m_ratio
   ,round(COALESCE( fin_43_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_43_3m_ratio
   ,round(COALESCE( fin_46_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_46_3m_ratio
   ,round(COALESCE( fin_47_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_47_3m_ratio
   ,round(COALESCE( fin_48_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_48_3m_ratio
   ,round(COALESCE( fin_49_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_49_3m_ratio
   ,round(COALESCE( fin_5_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_5_3m_ratio
   ,round(COALESCE( fin_50_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_50_3m_ratio
   ,round(COALESCE( fin_51_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_51_3m_ratio
   ,round(COALESCE( fin_52_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_52_3m_ratio
   ,round(COALESCE( fin_6_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_6_3m_ratio
   ,round(COALESCE( fin_8_3m_cnt*1.000000/total_3m_cnt,0),6) as  fin_8_3m_ratio
    from(
    select device
        ,count(pkg) as total_3m_cnt
        ,count(case when cate_id='A13' then pkg else null end ) as A13_3m_cnt
        ,count(case when cate_id='A13_1' then pkg else null end ) as A13_1_3m_cnt
        ,count(case when cate_id='A13_2' then pkg else null end ) as A13_2_3m_cnt
        ,count(case when cate_id='A13_3' then pkg else null end ) as A13_3_3m_cnt
        ,count(case when cate_id='A13_4' then pkg else null end ) as A13_4_3m_cnt
        ,count(case when cate_id='A20' then pkg else null end ) as A20_3m_cnt
        ,count(case when cate_id='A20_1' then pkg else null end ) as A20_1_3m_cnt
        ,count(case when cate_id='A20_10' then pkg else null end ) as A20_10_3m_cnt
        ,count(case when cate_id='A20_11' then pkg else null end ) as A20_11_3m_cnt
        ,count(case when cate_id='A20_12' then pkg else null end ) as A20_12_3m_cnt
        ,count(case when cate_id='A20_2' then pkg else null end ) as A20_2_3m_cnt
        ,count(case when cate_id='A20_4' then pkg else null end ) as A20_4_3m_cnt
        ,count(case when cate_id='A20_5' then pkg else null end ) as A20_5_3m_cnt
        ,count(case when cate_id='A20_6' then pkg else null end ) as A20_6_3m_cnt
        ,count(case when cate_id='A20_8' then pkg else null end ) as A20_8_3m_cnt
        ,count(case when cate_id='A4' then pkg else null end ) as A4_3m_cnt
        ,count(case when cate_id='A4_1' then pkg else null end ) as A4_1_3m_cnt
        ,count(case when cate_id='A4_2' then pkg else null end ) as A4_2_3m_cnt
        ,count(case when cate_id='A4_3' then pkg else null end ) as A4_3_3m_cnt
        ,count(case when cate_id='A5' then pkg else null end ) as A5_3m_cnt
        ,count(case when cate_id='A5_1' then pkg else null end ) as A5_1_3m_cnt
        ,count(case when cate_id='A5_2' then pkg else null end ) as A5_2_3m_cnt
        ,count(case when cate_id='A5_3' then pkg else null end ) as A5_3_3m_cnt
        ,count(case when cate_id='A6' then pkg else null end ) as A6_3m_cnt
        ,count(case when cate_id='A6_1' then pkg else null end ) as A6_1_3m_cnt
        ,count(case when cate_id='A6_2' then pkg else null end ) as A6_2_3m_cnt
        ,count(case when cate_id='A6_3' then pkg else null end ) as A6_3_3m_cnt
        ,count(case when cate_id='A9' then pkg else null end ) as A9_3m_cnt
        ,count(case when cate_id='A9_1' then pkg else null end ) as A9_1_3m_cnt
        ,count(case when cate_id='A9_2' then pkg else null end ) as A9_2_3m_cnt
        ,count(case when cate_id='A9_3' then pkg else null end ) as A9_3_3m_cnt
        ,count(case when cate_id='A9_4' then pkg else null end ) as A9_4_3m_cnt
        ,count(case when cate_id='B6' then pkg else null end ) as B6_3m_cnt
        ,count(case when cate_id='B6_1' then pkg else null end ) as B6_1_3m_cnt
        ,count(case when cate_id='B6_2' then pkg else null end ) as B6_2_3m_cnt
        ,count(case when cate_id='B6_3' then pkg else null end ) as B6_3_3m_cnt
        ,count(case when cate_id='B6_4' then pkg else null end ) as B6_4_3m_cnt
        ,count(case when cate_id='B6_5' then pkg else null end ) as B6_5_3m_cnt
        ,count(case when cate_id='B6_6' then pkg else null end ) as B6_6_3m_cnt
        ,count(case when cate_id='B7' then pkg else null end ) as B7_3m_cnt
        ,count(case when cate_id='B7_1' then pkg else null end ) as B7_1_3m_cnt
        ,count(case when cate_id='B7_2' then pkg else null end ) as B7_2_3m_cnt
        ,count(case when cate_id='B8' then pkg else null end ) as B8_3m_cnt
        ,count(case when cate_id='B8_1' then pkg else null end ) as B8_1_3m_cnt
        ,count(case when cate_id='B8_2' then pkg else null end ) as B8_2_3m_cnt
        ,count(case when cate_id='B8_3' then pkg else null end ) as B8_3_3m_cnt
        ,count(case when cate_id='fin_12' then pkg else null end ) as fin_12_3m_cnt
        ,count(case when cate_id='fin_24' then pkg else null end ) as fin_24_3m_cnt
        ,count(case when cate_id='fin_25' then pkg else null end ) as fin_25_3m_cnt
        ,count(case when cate_id='fin_26' then pkg else null end ) as fin_26_3m_cnt
        ,count(case when cate_id='fin_27' then pkg else null end ) as fin_27_3m_cnt
        ,count(case when cate_id='fin_28' then pkg else null end ) as fin_28_3m_cnt
        ,count(case when cate_id='fin_29' then pkg else null end ) as fin_29_3m_cnt
        ,count(case when cate_id='fin_3' then pkg else null end ) as fin_3_3m_cnt
        ,count(case when cate_id='fin_30' then pkg else null end ) as fin_30_3m_cnt
        ,count(case when cate_id='fin_31' then pkg else null end ) as fin_31_3m_cnt
        ,count(case when cate_id='fin_32' then pkg else null end ) as fin_32_3m_cnt
        ,count(case when cate_id='fin_4' then pkg else null end ) as fin_4_3m_cnt
        ,count(case when cate_id='fin_43' then pkg else null end ) as fin_43_3m_cnt
        ,count(case when cate_id='fin_46' then pkg else null end ) as fin_46_3m_cnt
        ,count(case when cate_id='fin_47' then pkg else null end ) as fin_47_3m_cnt
        ,count(case when cate_id='fin_48' then pkg else null end ) as fin_48_3m_cnt
        ,count(case when cate_id='fin_49' then pkg else null end ) as fin_49_3m_cnt
        ,count(case when cate_id='fin_5' then pkg else null end ) as fin_5_3m_cnt
        ,count(case when cate_id='fin_50' then pkg else null end ) as fin_50_3m_cnt
        ,count(case when cate_id='fin_51' then pkg else null end ) as fin_51_3m_cnt
        ,count(case when cate_id='fin_52' then pkg else null end ) as fin_52_3m_cnt
        ,count(case when cate_id='fin_6' then pkg else null end ) as fin_6_3m_cnt
        ,count(case when cate_id='fin_8' then pkg else null end ) as fin_8_3m_cnt
    from raw_table where datediff(date,flag_day)<=90 group by device
    )t2
)

insert overwrite table $income_temp_features_all partition (day=${day})
select a.device
   ,a.agebin_1004

   ,b.fin_4_12m_ratio
   ,b.fin_24_12m_ratio
   ,b.fin_29_12m_ratio
   ,b.b7_12m_ratio
   ,b.a5_12m_ratio
   ,b.a13_2_12m_ratio
   ,b.a13_1_12m_ratio
   ,b.a13_3_12m_ratio
   ,b.a13_4_12m_ratio
   ,b.b8_12m_ratio
   ,b.a6_3_12m_ratio
   ,b.a6_2_12m_ratio
   ,b.a6_1_12m_ratio
   ,b.fin_47_12m_ratio
   ,b.fin_52_12m_ratio
   ,b.fin_49_12m_ratio
   ,b.fin_48_12m_ratio
   ,b.fin_51_12m_ratio
   ,b.a4_12m_ratio
   ,b.fin_50_12m_ratio
   ,b.b6_12m_ratio
   ,b.fin_31_12m_ratio
   ,b.fin_3_12m_ratio
   ,b.fin_30_12m_ratio
   ,b.a9_12m_ratio
   ,b.fin_43_12m_ratio
   ,b.a20_7_12m_ratio
   ,b.a20_9_12m_ratio
   ,b.fin_19_12m_ratio
   ,b.fin_13_12m_ratio
   ,b.a9_2_12m_ratio
   ,b.a9_1_12m_ratio
   ,b.a9_3_12m_ratio
   ,b.a9_4_12m_ratio
   ,b.fin_27_12m_ratio
   ,b.b6_1_12m_ratio
   ,b.b6_3_12m_ratio
   ,b.b6_6_12m_ratio
   ,b.b6_4_12m_ratio
   ,b.b6_5_12m_ratio
   ,b.b6_2_12m_ratio
   ,b.a4_3_12m_ratio
   ,b.a4_1_12m_ratio
   ,b.a4_2_12m_ratio
   ,b.fin_46_12m_ratio
   ,b.a6_12m_ratio
   ,b.b8_2_12m_ratio
   ,b.b8_1_12m_ratio
   ,b.b8_3_12m_ratio
   ,b.a13_12m_ratio
   ,b.a5_2_12m_ratio
   ,b.a5_1_12m_ratio
   ,b.a5_3_12m_ratio
   ,b.b7_2_12m_ratio
   ,b.b7_1_12m_ratio
   ,b.a20_4_12m_ratio
   ,b.a20_1_12m_ratio
   ,b.fin_32_12m_ratio
   ,b.fin_28_12m_ratio
   ,b.fin_26_12m_ratio
   ,b.fin_25_12m_ratio
   ,b.fin_5_12m_ratio
   ,b.fin_6_12m_ratio

   ,c.fin_6_6m_ratio
   ,c.fin_5_6m_ratio
   ,c.fin_25_6m_ratio
   ,c.fin_26_6m_ratio
   ,c.a5_3_6m_ratio
   ,c.a5_1_6m_ratio
   ,c.a5_2_6m_ratio
   ,c.a6_1_6m_ratio
   ,c.a6_2_6m_ratio
   ,c.a6_3_6m_ratio
   ,c.a9_6m_ratio
   ,c.fin_32_6m_ratio
   ,c.fin_28_6m_ratio
   ,c.a20_1_6m_ratio
   ,c.a20_11_6m_ratio
   ,c.a20_6_6m_ratio
   ,c.a20_10_6m_ratio
   ,c.a20_8_6m_ratio
   ,c.a20_5_6m_ratio
   ,c.a20_2_6m_ratio
   ,c.a20_4_6m_ratio
   ,c.fin_50_6m_ratio
   ,c.a20_12_6m_ratio
   ,c.fin_51_6m_ratio
   ,c.fin_52_6m_ratio
   ,c.fin_48_6m_ratio
   ,c.fin_49_6m_ratio
   ,c.fin_47_6m_ratio
   ,c.fin_30_6m_ratio
   ,c.fin_31_6m_ratio
   ,c.a4_6m_ratio
   ,c.b6_2_6m_ratio
   ,c.b6_5_6m_ratio
   ,c.b6_6_6m_ratio
   ,c.b6_4_6m_ratio
   ,c.b6_1_6m_ratio
   ,c.b6_3_6m_ratio
   ,c.b7_6m_ratio
   ,c.fin_40_6m_ratio
   ,c.b8_6m_ratio
   ,c.a13_6m_ratio
   ,c.fin_22_6m_ratio
   ,c.fin_19_6m_ratio
   ,c.a20_9_6m_ratio
   ,c.fin_2_6m_ratio
   ,c.a8_6_6m_ratio
   ,c.tgi1_5_3_6m_ratio
   ,c.a20_7_6m_ratio
   ,c.fin_43_6m_ratio
   ,c.fin_36_6m_ratio
   ,c.fin_39_6m_ratio
   ,c.tgi1_5_1_6m_ratio
   ,c.tgi1_5_2_6m_ratio
   ,c.a14_6m_ratio
   ,c.tgi1_5_0_6m_ratio
   ,c.a2_1_6m_ratio
   ,c.a8_6m_ratio
   ,c.b12_6m_ratio
   ,c.b11_6m_ratio
   ,c.a18_6m_ratio
   ,c.tgi1_2_3_6m_ratio
   ,c.a2_6_6m_ratio
   ,c.fin_13_6m_ratio
   ,c.a7_4_6m_ratio
   ,c.a7_1_6m_ratio
   ,c.a8_4_6m_ratio
   ,c.a13_3_6m_ratio
   ,c.a13_4_6m_ratio
   ,c.a13_1_6m_ratio
   ,c.a13_2_6m_ratio
   ,c.fin_3_6m_ratio
   ,c.fin_15_6m_ratio
   ,c.b7_1_6m_ratio
   ,c.b7_2_6m_ratio
   ,c.b8_3_6m_ratio
   ,c.b8_1_6m_ratio
   ,c.b8_2_6m_ratio
   ,c.b6_6m_ratio
   ,c.a4_1_6m_ratio
   ,c.a4_2_6m_ratio
   ,c.a4_3_6m_ratio
   ,c.fin_27_6m_ratio
   ,c.fin_46_6m_ratio
   ,c.fin_29_6m_ratio
   ,c.a20_6m_ratio
   ,c.a9_4_6m_ratio
   ,c.a9_3_6m_ratio
   ,c.a9_2_6m_ratio
   ,c.a9_1_6m_ratio
   ,c.a6_6m_ratio
   ,c.a5_6m_ratio
   ,c.fin_24_6m_ratio
   ,c.fin_4_6m_ratio

   ,d.fin_26_3m_ratio
   ,d.fin_25_3m_ratio
   ,d.fin_27_3m_ratio
   ,d.fin_6_3m_ratio
   ,d.fin_5_3m_ratio
   ,d.a5_3m_ratio
   ,d.a9_1_3m_ratio
   ,d.a9_3_3m_ratio
   ,d.a9_4_3m_ratio
   ,d.a9_2_3m_ratio
   ,d.b8_1_3m_ratio
   ,d.b8_2_3m_ratio
   ,d.b8_3_3m_ratio
   ,d.a20_4_3m_ratio
   ,d.a20_12_3m_ratio
   ,d.a20_2_3m_ratio
   ,d.a20_10_3m_ratio
   ,d.a20_5_3m_ratio
   ,d.a20_6_3m_ratio
   ,d.a20_8_3m_ratio
   ,d.a20_1_3m_ratio
   ,d.a20_11_3m_ratio
   ,d.fin_49_3m_ratio
   ,d.fin_47_3m_ratio
   ,d.fin_48_3m_ratio
   ,d.fin_51_3m_ratio
   ,d.fin_52_3m_ratio
   ,d.a4_2_3m_ratio
   ,d.a4_3_3m_ratio
   ,d.a4_1_3m_ratio
   ,d.fin_50_3m_ratio
   ,d.b7_1_3m_ratio
   ,d.b7_2_3m_ratio
   ,d.b6_5_3m_ratio
   ,d.b6_3_3m_ratio
   ,d.b6_4_3m_ratio
   ,d.b6_6_3m_ratio
   ,d.b6_1_3m_ratio
   ,d.b6_2_3m_ratio
   ,d.a6_3_3m_ratio
   ,d.a6_2_3m_ratio
   ,d.a6_1_3m_ratio
   ,d.a13_2_3m_ratio
   ,d.a13_4_3m_ratio
   ,d.a13_1_3m_ratio
   ,d.a13_3_3m_ratio
   ,d.fin_43_3m_ratio
   ,d.fin_3_3m_ratio
   ,d.fin_12_3m_ratio
   ,d.a13_3m_ratio
   ,d.fin_8_3m_ratio
   ,d.a6_3m_ratio
   ,d.b6_3m_ratio
   ,d.b7_3m_ratio
   ,d.fin_29_3m_ratio
   ,d.fin_30_3m_ratio
   ,d.fin_31_3m_ratio
   ,d.fin_28_3m_ratio
   ,d.fin_32_3m_ratio
   ,d.fin_24_3m_ratio
   ,d.fin_4_3m_ratio
   ,d.a9_3m_ratio
   ,d.a5_2_3m_ratio
   ,d.a5_1_3m_ratio
   ,d.a5_3_3m_ratio
   ,d.a4_3m_ratio
   ,d.fin_46_3m_ratio
   ,d.a20_3m_ratio
   ,d.b8_3m_ratio

   ,e.fin_7_trend
   ,e.fin_4_trend
   ,e.tgi1_3_0_trend
   ,e.b8_trend
   ,e.tgi1_3_1_trend
   ,e.fin_11_trend
   ,e.fin_20_trend
   ,e.fin_12_trend
   ,e.a7_trend
   ,e.fin_15_trend
   ,e.tgi1_1_2_trend
   ,e.tgi1_4_0_trend
   ,e.a4_3_trend
   ,e.fin_17_trend
   ,e.a2_trend
   ,e.tgi1_1_1_trend
   ,e.fin_25_trend
   ,e.b6_4_trend
   ,e.a9_trend
   ,e.fin_23_trend
   ,e.b7_trend
   ,e.a5_2_trend
   ,e.fin_32_trend
   ,e.a18_trend
   ,e.a14_trend
   ,e.fin_1_trend
   ,e.fin_28_trend
   ,e.a8_trend
   ,e.a4_1_trend
   ,e.fin_39_trend
   ,e.fin_36_trend
   ,e.fin_16_trend
   ,e.fin_27_trend
   ,e.a13_3_trend
   ,e.b6_3_trend
   ,e.a13_2_trend
   ,e.a11_trend
   ,e.a5_3_trend
   ,e.a20_2_trend
   ,e.a20_trend
   ,e.fin_21_trend
   ,e.a1_trend
   ,e.tgi1_2_3_trend
   ,e.fin_40_trend
   ,e.a8_1_trend
   ,e.fin_48_trend
   ,e.fin_2_trend
   ,e.a10_2_trend
   ,e.a9_4_trend
   ,e.fin_52_trend
   ,e.a12_trend
   ,e.tgi1_4_1_trend
   ,e.a20_3_trend
   ,e.a6_1_trend
   ,e.fin_35_trend
   ,e.fin_33_trend
   ,e.tgi1_2_0_trend
   ,e.a20_8_trend
   ,e.b10_trend
   ,e.fin_44_trend
   ,e.fin_41_trend
   ,e.a2_2_trend
   ,e.a5_1_trend
   ,e.tgi1_4_2_trend
   ,e.fin_34_trend
   ,e.a6_2_trend
   ,e.a20_4_trend
   ,e.fin_19_trend
   ,e.fin_51_trend
   ,e.tgi1_3_2_trend
   ,e.a10_1_trend
   ,e.a14_3_trend
   ,e.a8_5_trend
   ,e.fin_22_trend
   ,e.a2_4_trend
   ,e.b11_trend
   ,e.a13_trend
   ,e.b6_trend
   ,e.a19_trend
   ,e.a8_2_trend
   ,e.a20_11_trend
   ,e.fin_38_trend
   ,e.a7_1_trend
   ,e.a20_9_trend
   ,e.fin_24_trend
   ,e.a20_12_trend
   ,e.tgi1_1_3_trend
   ,e.a20_5_trend
   ,e.a9_1_trend
   ,e.fin_45_trend
   ,e.a2_3_trend
   ,e.fin_42_trend
   ,e.fin_30_trend
   ,e.b1_trend
   ,e.fin_50_trend
   ,e.a10_trend
   ,e.b2_trend
   ,e.a14_1_trend
   ,e.b12_trend
   ,e.a2_1_trend
   ,e.a6_trend
   ,e.a9_2_trend
   ,e.a3_trend
   ,e.a15_trend
   ,e.b3_trend
   ,e.a2_7_trend
   ,e.b9_trend
   ,e.a2_5_trend
   ,e.a7_2_trend
   ,e.fin_3_trend
   ,e.b4_trend
   ,e.fin_26_trend
   ,e.a2_6_trend
   ,e.a17_trend
   ,e.b6_5_trend
   ,e.fin_9_trend
   ,e.b7_1_trend
   ,e.a6_3_trend
   ,e.tgi1_1_0_trend
   ,e.fin_31_trend
   ,e.a20_10_trend
   ,e.fin_13_trend
   ,e.b8_1_trend
   ,e.a20_6_trend
   ,e.a9_3_trend
   ,e.b6_2_trend
   ,e.fin_43_trend
   ,e.fin_47_trend
   ,e.b13_trend
   ,e.b7_2_trend
   ,e.fin_46_trend
   ,e.a13_4_trend
   ,e.a2_8_trend
   ,e.fin_29_trend
   ,e.a10_3_trend
   ,e.a20_7_trend
   ,e.b5_trend
   ,e.b6_6_trend
   ,e.fin_49_trend
   ,e.a5_trend
   ,e.a20_1_trend
   ,e.fin_14_trend
   ,e.a7_3_trend
   ,e.fin_5_trend
   ,e.tgi1_5_2_trend
   ,e.a4_2_trend
   ,e.a7_4_trend
   ,e.a8_6_trend
   ,e.b6_1_trend
   ,e.fin_8_trend
   ,e.a13_1_trend
   ,e.tgi1_5_3_trend
   ,e.fin_37_trend
   ,e.b8_3_trend
   ,e.b8_2_trend
   ,e.fin_10_trend
   ,e.tgi1_5_0_trend
   ,e.a8_3_trend
   ,e.tgi1_5_1_trend
   ,e.a4_trend
   ,e.tgi1_2_1_trend
   ,e.tgi1_2_2_trend
   ,e.fin_6_trend
   ,e.fin_18_trend
   ,e.a8_4_trend
from (select device,agebin_1004 from $device_profile_label_full_par where version='$last_par') a
inner join income_temp_ratio_features_12m b
on a.device=b.device
left join income_temp_ratio_features_6m c
on a.device=c.device
left join income_temp_ratio_features_3m d
on a.device=d.device
left join income_temp_cate_embedding_cosin e
on a.device=e.device
"
# + pmml文件, 特征为除 device、agebin_1004、day以外的所有字段
# 生成最终表: dm_mobeye_master.device_profile_lable_income
# 包含字段: device,agebin_1004,income,day

spark2-submit \
--master yarn \
--deploy-mode cluster \
--name "收入回归模型" \
--executor-cores 3 \
--executor-memory 9g \
--driver-memory 2g \
--conf "spark.dynamicAllocation.enabled=true" \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.sql.shuffle.partitions=1500" \
--conf "spark.shuffle.service.enabled=true" \
--class com.youzu.mob.score.IncomeMobeye \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $day $income_temp_features_all $device_profile_lable_income

#逻辑自洽 如果年龄<18, 收入记为0
hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
insert overwrite table $device_profile_lable_income partition (day=${day})
select device
  ,agebin_1004
  ,case when income<0 then null when agebin_1004=9 then 0 else income end as income
from $device_profile_lable_income
where day=${day}
"



