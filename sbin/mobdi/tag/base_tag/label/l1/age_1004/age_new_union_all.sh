#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_two_month=$(date -d "${day} -2 month" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
age_new_ratio_features_12m="$tmpdb.age_new_ratio_features_12m"
age_new_ratio_features_6m="$tmpdb.age_new_ratio_features_6m"
age_new_ratio_features_3m="$tmpdb.age_new_ratio_features_3m"
age_new_newIns_recency_features="$tmpdb.age_new_newIns_recency_features"
age_new_Ins_recency_features="$tmpdb.age_new_Ins_recency_features"
age_new_install_cnt_6mv12m="$tmpdb.age_new_install_cnt_6mv12m"
age_new_pkg_install_12="$tmpdb.age_new_pkg_install_12"
age_new_applist_install_bycate_id_whether="$tmpdb.age_new_applist_install_bycate_id_whether"
age_new_active_recency_features="$tmpdb.age_new_active_recency_features"
age_new_active_days_12="$tmpdb.age_new_active_days_12"
age_new_active_days_6="$tmpdb.age_new_active_days_6"
age_new_active_days_3="$tmpdb.age_new_active_days_3"
age_new_active_days_1="$tmpdb.age_new_active_days_1"
age_new_embedding_cosin_bycate="$tmpdb.age_new_embedding_cosin_bycate"
age_new_topic_wgt="$tmpdb.age_new_topic_wgt"

age_new_age_features_all="$tmpdb.age_new_features_all"

hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table $age_new_age_features_all partition (day = $day)
select 
a.device
,cate2_12m_cnt
,cate7006_001_12m_cnt
,cate7015_005_12m_cnt
,cate7016_001_12m_cnt
,cate7016_002_12m_cnt
,fin_46_12m_cnt
,tgi1_18_0_12m_cnt
,tgi1_25_34_1_12m_cnt
,tgi1_55_0_12m_cnt
,tgi2_35_44_6_12m_cnt
,cate26_12m_cnt
,cate7002_001_12m_cnt
,cate7005_001_12m_cnt
,cate7005_002_12m_cnt
,cate7007_001_12m_cnt
,cate7009_001_12m_cnt
,cate7014_007_12m_cnt
,cate7019_122_12m_cnt
,cate7019_127_12m_cnt
,tgi1_18_1_12m_cnt
,tgi1_18_24_6_12m_cnt
,tgi1_25_34_4_12m_cnt
,tgi1_35_44_0_12m_cnt
,tgi2_18_2_12m_cnt
,tgi2_18_24_2_12m_cnt
,tgi2_45_54_0_12m_cnt
,cate14_12m_ratio
,cate7001_003_12m_ratio
,cate7001_006_12m_ratio
,cate7001_013_12m_ratio
,cate7002_005_12m_ratio
,cate7002_007_12m_ratio
,cate7003_001_12m_ratio
,cate7005_001_12m_ratio
,cate7005_002_12m_ratio
,cate7006_001_12m_ratio
,cate7006_003_12m_ratio
,cate7008_003_12m_ratio
,cate7009_001_12m_ratio
,cate7010_002_12m_ratio
,cate7011_995_12m_ratio
,cate7011_997_12m_ratio
,cate7012_003_12m_ratio
,cate7012_007_12m_ratio
,cate7014_009_12m_ratio
,cate7014_010_12m_ratio
,cate7014_017_12m_ratio
,cate7015_004_12m_ratio
,cate7015_006_12m_ratio
,cate7015_019_12m_ratio
,cate7016_002_12m_ratio
,cate7016_003_12m_ratio
,cate7019_136_12m_ratio
,fin_27_12m_ratio
,tgi1_18_0_12m_ratio
,tgi1_18_2_12m_ratio
,tgi1_18_24_3_12m_ratio
,tgi1_35_44_1_12m_ratio
,tgi1_55_6_12m_ratio
,tgi2_18_1_12m_ratio
,tgi1_18_6_12m_ratio
,tgi2_18_24_0_12m_ratio
,tgi2_18_24_1_12m_ratio
,tgi2_18_24_2_12m_ratio
,tgi2_18_24_4_12m_ratio
,tgi2_18_4_12m_ratio
,tgi2_18_5_12m_ratio
,tgi2_18_7_12m_ratio
,tgi2_25_34_3_12m_ratio
,tgi2_25_34_4_12m_ratio
,tgi2_25_34_5_12m_ratio
,tgi2_25_34_6_12m_ratio
,tgi2_35_44_2_12m_ratio
,tgi2_35_44_3_12m_ratio
,tgi2_35_44_4_12m_ratio
,tgi2_35_44_5_12m_ratio
,tgi2_45_54_2_12m_ratio
,tgi2_45_54_3_12m_ratio
,tgi2_45_54_4_12m_ratio
,tgi2_45_54_5_12m_ratio
,tgi2_55_2_12m_ratio
,tgi2_55_3_12m_ratio
,tgi2_55_4_12m_ratio
,tgi2_55_5_12m_ratio
,tgi2_55_7_12m_ratio

,COALESCE(cate2_6m_cnt,-999) as cate2_6m_cnt
,COALESCE(cate7006_001_6m_cnt,-999) as cate7006_001_6m_cnt
,COALESCE(cate7015_005_6m_cnt,-999) as cate7015_005_6m_cnt
,COALESCE(cate7016_001_6m_cnt,-999) as cate7016_001_6m_cnt
,COALESCE(cate7016_002_6m_cnt,-999) as cate7016_002_6m_cnt
,COALESCE(cate7019_122_6m_cnt,-999) as cate7019_122_6m_cnt
,COALESCE(fin_46_6m_cnt,-999) as fin_46_6m_cnt
,COALESCE(tgi1_25_34_1_6m_cnt,-999) as tgi1_25_34_1_6m_cnt
,COALESCE(tgi1_55_0_6m_cnt,-999) as tgi1_55_0_6m_cnt
,COALESCE(tgi2_35_44_6_6m_cnt,-999) as tgi2_35_44_6_6m_cnt
,COALESCE(tgi2_45_54_0_6m_cnt,-999) as tgi2_45_54_0_6m_cnt
,COALESCE(tgi1_18_0_6m_cnt,-999) as tgi1_18_0_6m_cnt
,COALESCE(tgi1_18_24_6_6m_cnt,-999) as tgi1_18_24_6_6m_cnt
,COALESCE(cate22_6m_ratio,-999) as cate22_6m_ratio
,COALESCE(cate7001_003_6m_ratio,-999) as cate7001_003_6m_ratio
,COALESCE(cate7002_003_6m_ratio,-999) as cate7002_003_6m_ratio
,COALESCE(cate7002_005_6m_ratio,-999) as cate7002_005_6m_ratio
,COALESCE(cate7003_001_6m_ratio,-999) as cate7003_001_6m_ratio
,COALESCE(cate7005_002_6m_ratio,-999) as cate7005_002_6m_ratio
,COALESCE(cate7006_003_6m_ratio,-999) as cate7006_003_6m_ratio
,COALESCE(cate7012_001_6m_ratio,-999) as cate7012_001_6m_ratio
,COALESCE(cate7012_003_6m_ratio,-999) as cate7012_003_6m_ratio
,COALESCE(cate7012_013_6m_ratio,-999) as cate7012_013_6m_ratio
,COALESCE(cate7012_014_6m_ratio,-999) as cate7012_014_6m_ratio
,COALESCE(cate7014_009_6m_ratio,-999) as cate7014_009_6m_ratio
,COALESCE(cate7014_010_6m_ratio,-999) as cate7014_010_6m_ratio
,COALESCE(cate7014_017_6m_ratio,-999) as cate7014_017_6m_ratio
,COALESCE(cate7015_004_6m_ratio,-999) as cate7015_004_6m_ratio
,COALESCE(fin_27_6m_ratio,-999) as fin_27_6m_ratio
,COALESCE(tgi1_18_0_6m_ratio,-999) as tgi1_18_0_6m_ratio
,COALESCE(tgi2_18_24_0_6m_ratio,-999) as tgi2_18_24_0_6m_ratio
,COALESCE(tgi2_18_24_1_6m_ratio,-999) as tgi2_18_24_1_6m_ratio
,COALESCE(tgi2_18_4_6m_ratio,-999) as tgi2_18_4_6m_ratio
,COALESCE(tgi2_25_34_3_6m_ratio,-999) as tgi2_25_34_3_6m_ratio
,COALESCE(tgi2_25_34_6_6m_ratio,-999) as tgi2_25_34_6_6m_ratio
,COALESCE(tgi2_35_44_3_6m_ratio,-999) as tgi2_35_44_3_6m_ratio
,COALESCE(tgi2_35_44_5_6m_ratio,-999) as tgi2_35_44_5_6m_ratio
,COALESCE(tgi2_55_2_6m_ratio,-999) as tgi2_55_2_6m_ratio


,COALESCE(tgi1_18_24_6_3m_cnt,-999) as tgi1_18_24_6_3m_cnt
,COALESCE(tgi1_18_0_3m_ratio,-999) as tgi1_18_0_3m_ratio


,COALESCE(cate7011_998_newIns_first_min_diff,-99) as cate7011_998_newIns_first_min_diff
,COALESCE(tgi1_18_24_0_newIns_first_min_diff,-99) as tgi1_18_24_0_newIns_first_min_diff
,COALESCE(tgi1_25_34_0_newIns_first_min_diff,-99) as tgi1_25_34_0_newIns_first_min_diff
,COALESCE(tgi1_35_44_1_newIns_first_min_diff,-99) as tgi1_35_44_1_newIns_first_min_diff
,COALESCE(tgi1_55_6_newIns_first_min_diff,-99) as tgi1_55_6_newIns_first_min_diff
,COALESCE(tgi2_18_0_newIns_first_min_diff,-99) as tgi2_18_0_newIns_first_min_diff
,COALESCE(tgi2_55_1_newIns_first_min_diff,-99) as tgi2_55_1_newIns_first_min_diff
,COALESCE(cate7016_002_newIns_first_max_diff,-99) as cate7016_002_newIns_first_max_diff
,COALESCE(tgi2_18_24_4_newIns_first_max_diff,-99) as tgi2_18_24_4_newIns_first_max_diff
,COALESCE(tgi2_18_24_5_newIns_first_max_diff,-99) as tgi2_18_24_5_newIns_first_max_diff
,COALESCE(tgi2_25_34_0_newIns_first_max_diff,-99) as tgi2_25_34_0_newIns_first_max_diff
,COALESCE(tgi2_25_34_1_newIns_first_max_diff,-99) as tgi2_25_34_1_newIns_first_max_diff
,COALESCE(tgi2_25_34_6_newIns_first_max_diff,-99) as tgi2_25_34_6_newIns_first_max_diff
,COALESCE(tgi2_55_0_newIns_first_max_diff,-99) as tgi2_55_0_newIns_first_max_diff


,COALESCE(cate7006_001_install_first_min_diff,-99) as cate7006_001_install_first_min_diff
,COALESCE(tgi1_18_24_5_install_first_min_diff,-99) as tgi1_18_24_5_install_first_min_diff
,COALESCE(tgi1_25_34_0_install_first_min_diff,-99) as tgi1_25_34_0_install_first_min_diff
,COALESCE(tgi1_35_44_1_install_first_min_diff,-99) as tgi1_35_44_1_install_first_min_diff
,COALESCE(tgi1_35_44_5_install_first_min_diff,-99) as tgi1_35_44_5_install_first_min_diff
,COALESCE(tgi1_45_54_1_install_first_min_diff,-99) as tgi1_45_54_1_install_first_min_diff
,COALESCE(tgi1_55_0_install_first_min_diff,-99) as tgi1_55_0_install_first_min_diff
,COALESCE(tgi2_45_54_5_install_first_min_diff,-99) as tgi2_45_54_5_install_first_min_diff
,COALESCE(cate7001_002_install_first_max_diff,-99) as cate7001_002_install_first_max_diff
,COALESCE(cate7008_002_install_first_max_diff,-99) as cate7008_002_install_first_max_diff
,COALESCE(tgi1_18_0_install_first_max_diff,-99) as tgi1_18_0_install_first_max_diff
,COALESCE(tgi1_18_1_install_first_max_diff,-99) as tgi1_18_1_install_first_max_diff
,COALESCE(tgi1_45_54_0_install_first_max_diff,-99) as tgi1_45_54_0_install_first_max_diff
,COALESCE(tgi1_55_6_install_first_max_diff,-99) as tgi1_55_6_install_first_max_diff
,COALESCE(tgi2_25_34_5_install_first_max_diff,-99) as tgi2_25_34_5_install_first_max_diff
,COALESCE(tgi2_35_44_5_install_first_max_diff,-99) as tgi2_35_44_5_install_first_max_diff
,COALESCE(tgi2_55_1_install_first_max_diff,-99) as tgi2_55_1_install_first_max_diff


,COALESCE(cate2_6mv12m,-999) as cate2_6mv12m
,COALESCE(cate7006_001_6mv12m,-999) as cate7006_001_6mv12m
,COALESCE(cate7015_005_6mv12m,-999) as cate7015_005_6mv12m
,COALESCE(cate7016_001_6mv12m,-999) as cate7016_001_6mv12m
,COALESCE(cate7016_002_6mv12m,-999) as cate7016_002_6mv12m
,COALESCE(cate7019_122_6mv12m,-999) as cate7019_122_6mv12m
,COALESCE(fin_46_6mv12m,-999) as fin_46_6mv12m
,COALESCE(tgi1_18_0_6mv12m,-999) as tgi1_18_0_6mv12m
,COALESCE(tgi1_25_34_1_6mv12m,-999) as tgi1_25_34_1_6mv12m
,COALESCE(tgi1_55_0_6mv12m,-999) as tgi1_55_0_6mv12m
,COALESCE(tgi2_35_44_6_6mv12m,-999) as tgi2_35_44_6_6mv12m
,COALESCE(tgi2_45_54_0_6mv12m,-999) as tgi2_45_54_0_6mv12m


,COALESCE(cate14_12,0) as cate14_12
,COALESCE(cate26_12,0) as cate26_12
,COALESCE(cate7002_007_12,0) as cate7002_007_12
,COALESCE(cate7006_001_12,0) as cate7006_001_12
,COALESCE(cate7008_010_12,0) as cate7008_010_12
,COALESCE(cate7009_001_12,0) as cate7009_001_12
,COALESCE(cate7010_001_12,0) as cate7010_001_12
,COALESCE(cate7011_993_12,0) as cate7011_993_12
,COALESCE(cate7014_002_12,0) as cate7014_002_12
,COALESCE(cate7019_122_12,0) as cate7019_122_12
,COALESCE(cate7019_127_12,0) as cate7019_127_12
,COALESCE(tgi1_18_1_12,0) as tgi1_18_1_12
,COALESCE(tgi1_18_2_12,0) as tgi1_18_2_12
,COALESCE(tgi1_25_34_4_12,0) as tgi1_25_34_4_12
,COALESCE(tgi1_35_44_5_12,0) as tgi1_35_44_5_12
,COALESCE(tgi2_18_0_12,0) as tgi2_18_0_12
,COALESCE(tgi2_18_24_2_12,0) as tgi2_18_24_2_12
,COALESCE(tgi2_18_24_4_12,0) as tgi2_18_24_4_12
,COALESCE(tgi2_18_7_12,0) as tgi2_18_7_12
,COALESCE(tgi2_25_34_5_12,0) as tgi2_25_34_5_12
,COALESCE(tgi2_25_34_6_12,0) as tgi2_25_34_6_12
,COALESCE(tgi2_35_44_4_12,0) as tgi2_35_44_4_12
,COALESCE(tgi2_35_44_6_12,0) as tgi2_35_44_6_12


,case when cate17_active_max_day_diff >0 then cate17_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate17=0 then -99 when cate17_active_max_day_diff is null then -95 else  cate17_active_max_day_diff end as cate17_active_max_day_diff
,case when cate7001_002_active_max_day_diff >0 then cate7001_002_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate7001_002=0 then -99 when cate7001_002_active_max_day_diff is null then -95 else  cate7001_002_active_max_day_diff end as cate7001_002_active_max_day_diff
,case when cate7005_001_active_max_day_diff >0 then cate7005_001_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate7005_001=0 then -99 when cate7005_001_active_max_day_diff is null then -95 else  cate7005_001_active_max_day_diff end as cate7005_001_active_max_day_diff
,case when cate7010_001_active_max_day_diff >0 then cate7010_001_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate7010_001=0 then -99 when cate7010_001_active_max_day_diff is null then -95 else  cate7010_001_active_max_day_diff end as cate7010_001_active_max_day_diff
,case when cate7011_998_active_max_day_diff >0 then cate7011_998_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate7011_998=0 then -99 when cate7011_998_active_max_day_diff is null then -95 else  cate7011_998_active_max_day_diff end as cate7011_998_active_max_day_diff
,case when cate7013_008_active_max_day_diff >0 then cate7013_008_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate7013_008=0 then -99 when cate7013_008_active_max_day_diff is null then -95 else  cate7013_008_active_max_day_diff end as cate7013_008_active_max_day_diff
,case when cate7015_001_active_max_day_diff >0 then cate7015_001_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_cate7015_001=0 then -99 when cate7015_001_active_max_day_diff is null then -95 else  cate7015_001_active_max_day_diff end as cate7015_001_active_max_day_diff
,case when fin_46_active_max_day_diff >0 then fin_46_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_fin_46=0 then -99 when fin_46_active_max_day_diff is null then -95 else  fin_46_active_max_day_diff end as fin_46_active_max_day_diff
,case when tgi1_18_0_active_max_day_diff >0 then tgi1_18_0_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_18_0=0 then -99 when tgi1_18_0_active_max_day_diff is null then -95 else  tgi1_18_0_active_max_day_diff end as tgi1_18_0_active_max_day_diff
,case when tgi1_35_44_1_active_max_day_diff >0 then tgi1_35_44_1_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_35_44_1=0 then -99 when tgi1_35_44_1_active_max_day_diff is null then -95 else  tgi1_35_44_1_active_max_day_diff end as tgi1_35_44_1_active_max_day_diff
,case when tgi1_35_44_5_active_max_day_diff >0 then tgi1_35_44_5_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_35_44_5=0 then -99 when tgi1_35_44_5_active_max_day_diff is null then -95 else  tgi1_35_44_5_active_max_day_diff end as tgi1_35_44_5_active_max_day_diff
,case when tgi1_45_54_5_active_max_day_diff >0 then tgi1_45_54_5_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_45_54_5=0 then -99 when tgi1_45_54_5_active_max_day_diff is null then -95 else  tgi1_45_54_5_active_max_day_diff end as tgi1_45_54_5_active_max_day_diff
,case when tgi2_18_0_active_max_day_diff >0 then tgi2_18_0_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_18_0=0 then -99 when tgi2_18_0_active_max_day_diff is null then -95 else  tgi2_18_0_active_max_day_diff end as tgi2_18_0_active_max_day_diff
,case when tgi2_18_24_5_active_max_day_diff >0 then tgi2_18_24_5_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_18_24_5=0 then -99 when tgi2_18_24_5_active_max_day_diff is null then -95 else  tgi2_18_24_5_active_max_day_diff end as tgi2_18_24_5_active_max_day_diff
,case when tgi2_25_34_5_active_max_day_diff >0 then tgi2_25_34_5_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_25_34_5=0 then -99 when tgi2_25_34_5_active_max_day_diff is null then -95 else  tgi2_25_34_5_active_max_day_diff end as tgi2_25_34_5_active_max_day_diff
,case when tgi2_35_44_5_active_max_day_diff >0 then tgi2_35_44_5_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_35_44_5=0 then -99 when tgi2_35_44_5_active_max_day_diff is null then -95 else  tgi2_35_44_5_active_max_day_diff end as tgi2_35_44_5_active_max_day_diff
,case when tgi2_35_44_6_active_max_day_diff >0 then tgi2_35_44_6_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_35_44_6=0 then -99 when tgi2_35_44_6_active_max_day_diff is null then -95 else  tgi2_35_44_6_active_max_day_diff end as tgi2_35_44_6_active_max_day_diff
,case when tgi2_45_54_1_active_max_day_diff >0 then tgi2_45_54_1_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_45_54_1=0 then -99 when tgi2_45_54_1_active_max_day_diff is null then -95 else  tgi2_45_54_1_active_max_day_diff end as tgi2_45_54_1_active_max_day_diff
,case when tgi2_55_0_active_max_day_diff >0 then tgi2_55_0_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_55_0=0 then -99 when tgi2_55_0_active_max_day_diff is null then -95 else  tgi2_55_0_active_max_day_diff end as tgi2_55_0_active_max_day_diff
,case when tgi2_55_1_active_max_day_diff >0 then tgi2_55_1_active_max_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_55_1=0 then -99 when tgi2_55_1_active_max_day_diff is null then -95 else  tgi2_55_1_active_max_day_diff end as tgi2_55_1_active_max_day_diff
,case when cate2_active_min_day_diff >0 then cate2_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_cate2=0 then -99 when cate2_active_min_day_diff is null then -95 else  cate2_active_min_day_diff end as cate2_active_min_day_diff
,case when cate22_active_min_day_diff >0 then cate22_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_cate22=0 then -99 when cate22_active_min_day_diff is null then -95 else  cate22_active_min_day_diff end as cate22_active_min_day_diff
,case when cate7011_993_active_min_day_diff >0 then cate7011_993_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_cate7011_993=0 then -99 when cate7011_993_active_min_day_diff is null then -95 else  cate7011_993_active_min_day_diff end as cate7011_993_active_min_day_diff
,case when cate7012_012_active_min_day_diff >0 then cate7012_012_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_cate7012_012=0 then -99 when cate7012_012_active_min_day_diff is null then -95 else  cate7012_012_active_min_day_diff end as cate7012_012_active_min_day_diff
,case when cate7019_107_active_min_day_diff >0 then cate7019_107_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_cate7019_107=0 then -99 when cate7019_107_active_min_day_diff is null then -95 else  cate7019_107_active_min_day_diff end as cate7019_107_active_min_day_diff
,case when tgi1_18_24_0_active_min_day_diff >0 then tgi1_18_24_0_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_18_24_0=0 then -99 when tgi1_18_24_0_active_min_day_diff is null then -95 else  tgi1_18_24_0_active_min_day_diff end as tgi1_18_24_0_active_min_day_diff
,case when tgi1_18_6_active_min_day_diff >0 then tgi1_18_6_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_18_6=0 then -99 when tgi1_18_6_active_min_day_diff is null then -95 else  tgi1_18_6_active_min_day_diff end as tgi1_18_6_active_min_day_diff
,case when tgi1_25_34_0_active_min_day_diff >0 then tgi1_25_34_0_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_25_34_0=0 then -99 when tgi1_25_34_0_active_min_day_diff is null then -95 else  tgi1_25_34_0_active_min_day_diff end as tgi1_25_34_0_active_min_day_diff
,case when tgi1_25_34_5_active_min_day_diff >0 then tgi1_25_34_5_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_25_34_5=0 then -99 when tgi1_25_34_5_active_min_day_diff is null then -95 else  tgi1_25_34_5_active_min_day_diff end as tgi1_25_34_5_active_min_day_diff
,case when tgi1_45_54_0_active_min_day_diff >0 then tgi1_45_54_0_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi1_45_54_0=0 then -99 when tgi1_45_54_0_active_min_day_diff is null then -95 else  tgi1_45_54_0_active_min_day_diff end as tgi1_45_54_0_active_min_day_diff
,case when tgi2_18_24_5_active_min_day_diff >0 then tgi2_18_24_5_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_18_24_5=0 then -99 when tgi2_18_24_5_active_min_day_diff is null then -95 else  tgi2_18_24_5_active_min_day_diff end as tgi2_18_24_5_active_min_day_diff
,case when tgi2_35_44_6_active_min_day_diff >0 then tgi2_35_44_6_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_35_44_6=0 then -99 when tgi2_35_44_6_active_min_day_diff is null then -95 else  tgi2_35_44_6_active_min_day_diff end as tgi2_35_44_6_active_min_day_diff
,case when tgi2_45_54_5_active_min_day_diff >0 then tgi2_45_54_5_active_min_day_diff when  flag_12m=0 then -999 when flag_12m_tgi2_45_54_5=0 then -99 when tgi2_45_54_5_active_min_day_diff is null then -95 else  tgi2_45_54_5_active_min_day_diff end as tgi2_45_54_5_active_min_day_diff


,COALESCE(tgi2_18_5_act_day_12m,-999) as tgi2_18_5_act_day_12m
,COALESCE(tgi2_18_6_act_day_12m,-999) as tgi2_18_6_act_day_12m
,COALESCE(tgi2_45_54_1_act_day_12m,-999) as tgi2_45_54_1_act_day_12m

,COALESCE(tgi2_18_0_act_day_6m,-999) as tgi2_18_0_act_day_6m

,COALESCE(tgi2_55_0_act_day_3m,-999) as tgi2_55_0_act_day_3m
,COALESCE(tgi2_18_24_0_act_day_3m,-999) as tgi2_18_24_0_act_day_3m

,COALESCE(tgi2_55_0_act_day_1m,-999) as tgi2_55_0_act_day_1m


,COALESCE(cate1_18_install_trend,-1) as cate1_18_install_trend
,COALESCE(cate10_18_24_install_trend,-1) as cate10_18_24_install_trend
,COALESCE(cate11_install_trend,-1) as cate11_install_trend
,COALESCE(cate15_55_install_trend,-1) as cate15_55_install_trend
,COALESCE(cate16_35_44_install_trend,-1) as cate16_35_44_install_trend
,COALESCE(cate18_55_install_trend,-1) as cate18_55_install_trend
,COALESCE(cate2_55_install_trend,-1) as cate2_55_install_trend
,COALESCE(cate21_45_54_install_trend,-1) as cate21_45_54_install_trend
,COALESCE(cate23_install_trend,-1) as cate23_install_trend
,COALESCE(cate24_install_trend,-1) as cate24_install_trend
,COALESCE(cate3_18_install_trend,-1) as cate3_18_install_trend
,COALESCE(cate5_55_install_trend,-1) as cate5_55_install_trend
,COALESCE(cate6_55_install_trend,-1) as cate6_55_install_trend
,COALESCE(cate7_55_install_trend,-1) as cate7_55_install_trend
,COALESCE(cate7001_011_55_install_trend,-1) as cate7001_011_55_install_trend
,COALESCE(cate7002_001_45_54_install_trend,-1) as cate7002_001_45_54_install_trend
,COALESCE(cate7002_002_18_install_trend,-1) as cate7002_002_18_install_trend
,COALESCE(cate7002_003_25_34_install_trend,-1) as cate7002_003_25_34_install_trend
,COALESCE(cate7002_004_55_install_trend,-1) as cate7002_004_55_install_trend
,COALESCE(cate7002_005_55_install_trend,-1) as cate7002_005_55_install_trend
,COALESCE(cate7002_007_18_install_trend,-1) as cate7002_007_18_install_trend
,COALESCE(cate7002_008_18_24_install_trend,-1) as cate7002_008_18_24_install_trend
,COALESCE(cate7002_010_18_24_install_trend,-1) as cate7002_010_18_24_install_trend
,COALESCE(cate7002_010_install_trend,-1) as cate7002_010_install_trend
,COALESCE(cate7003_002_25_34_install_trend,-1) as cate7003_002_25_34_install_trend
,COALESCE(cate7003_004_install_trend,-1) as cate7003_004_install_trend
,COALESCE(cate7003_006_18_24_install_trend,-1) as cate7003_006_18_24_install_trend
,COALESCE(cate7003_006_55_install_trend,-1) as cate7003_006_55_install_trend
,COALESCE(cate7003_008_55_install_trend,-1) as cate7003_008_55_install_trend
,COALESCE(cate7003_008_18_install_trend,-1) as cate7003_008_18_install_trend
,COALESCE(cate7004_002_25_34_install_trend,-1) as cate7004_002_25_34_install_trend
,COALESCE(cate7004_003_55_install_trend,-1) as cate7004_003_55_install_trend
,COALESCE(cate7004_004_18_24_install_trend,-1) as cate7004_004_18_24_install_trend
,COALESCE(cate7004_006_18_install_trend,-1) as cate7004_006_18_install_trend
,COALESCE(cate7004_007_18_install_trend,-1) as cate7004_007_18_install_trend
,COALESCE(cate7005_001_install_trend,-1) as cate7005_001_install_trend
,COALESCE(cate7005_002_55_install_trend,-1) as cate7005_002_55_install_trend
,COALESCE(cate7005_003_25_34_install_trend,-1) as cate7005_003_25_34_install_trend
,COALESCE(cate7005_005_install_trend,-1) as cate7005_005_install_trend
,COALESCE(cate7005_007_25_34_install_trend,-1) as cate7005_007_25_34_install_trend
,COALESCE(cate7005_008_55_install_trend,-1) as cate7005_008_55_install_trend
,COALESCE(cate7006_001_18_24_install_trend,-1) as cate7006_001_18_24_install_trend
,COALESCE(cate7006_003_35_44_install_trend,-1) as cate7006_003_35_44_install_trend
,COALESCE(cate7006_005_25_34_install_trend,-1) as cate7006_005_25_34_install_trend
,COALESCE(cate7007_001_18_24_install_trend,-1) as cate7007_001_18_24_install_trend
,COALESCE(cate7007_002_35_44_install_trend,-1) as cate7007_002_35_44_install_trend
,COALESCE(cate7007_003_18_24_install_trend,-1) as cate7007_003_18_24_install_trend
,COALESCE(cate7007_005_55_install_trend,-1) as cate7007_005_55_install_trend
,COALESCE(cate7008_001_55_install_trend,-1) as cate7008_001_55_install_trend
,COALESCE(cate7008_001_install_trend,-1) as cate7008_001_install_trend
,COALESCE(cate7008_004_18_24_install_trend,-1) as cate7008_004_18_24_install_trend
,COALESCE(cate7008_006_55_install_trend,-1) as cate7008_006_55_install_trend
,COALESCE(cate7008_007_install_trend,-1) as cate7008_007_install_trend
,COALESCE(cate7008_008_18_install_trend,-1) as cate7008_008_18_install_trend
,COALESCE(cate7008_009_18_install_trend,-1) as cate7008_009_18_install_trend
,COALESCE(cate7009_002_35_44_install_trend,-1) as cate7009_002_35_44_install_trend
,COALESCE(cate7009_003_35_44_install_trend,-1) as cate7009_003_35_44_install_trend
,COALESCE(cate7009_005_55_install_trend,-1) as cate7009_005_55_install_trend
,COALESCE(cate7009_007_18_install_trend,-1) as cate7009_007_18_install_trend
,COALESCE(cate7010_001_35_44_install_trend,-1) as cate7010_001_35_44_install_trend
,COALESCE(cate7010_003_18_24_install_trend,-1) as cate7010_003_18_24_install_trend
,COALESCE(cate7010_005_18_install_trend,-1) as cate7010_005_18_install_trend
,COALESCE(cate7010_006_55_install_trend,-1) as cate7010_006_55_install_trend
,COALESCE(cate7011_991_18_24_install_trend,-1) as cate7011_991_18_24_install_trend
,COALESCE(cate7011_993_18_install_trend,-1) as cate7011_993_18_install_trend
,COALESCE(cate7011_998_55_install_trend,-1) as cate7011_998_55_install_trend
,COALESCE(cate7011_999_18_24_install_trend,-1) as cate7011_999_18_24_install_trend
,COALESCE(cate7012_002_install_trend,-1) as cate7012_002_install_trend
,COALESCE(cate7012_005_35_44_install_trend,-1) as cate7012_005_35_44_install_trend
,COALESCE(cate7012_008_18_24_install_trend,-1) as cate7012_008_18_24_install_trend
,COALESCE(cate7012_008_install_trend,-1) as cate7012_008_install_trend
,COALESCE(cate7012_009_18_install_trend,-1) as cate7012_009_18_install_trend
,COALESCE(cate7012_012_18_install_trend,-1) as cate7012_012_18_install_trend
,COALESCE(cate7013_001_25_34_install_trend,-1) as cate7013_001_25_34_install_trend
,COALESCE(cate7013_006_45_54_install_trend,-1) as cate7013_006_45_54_install_trend
,COALESCE(cate7013_007_55_install_trend,-1) as cate7013_007_55_install_trend
,COALESCE(cate7014_002_35_44_install_trend,-1) as cate7014_002_35_44_install_trend
,COALESCE(cate7014_002_install_trend,-1) as cate7014_002_install_trend
,COALESCE(cate7014_006_18_install_trend,-1) as cate7014_006_18_install_trend
,COALESCE(cate7014_007_install_trend,-1) as cate7014_007_install_trend
,COALESCE(cate7014_008_18_install_trend,-1) as cate7014_008_18_install_trend
,COALESCE(cate7014_011_18_install_trend,-1) as cate7014_011_18_install_trend
,COALESCE(cate7014_014_45_54_install_trend,-1) as cate7014_014_45_54_install_trend
,COALESCE(cate7014_015_35_44_install_trend,-1) as cate7014_015_35_44_install_trend
,COALESCE(cate7014_016_55_install_trend,-1) as cate7014_016_55_install_trend
,COALESCE(cate7015_001_25_34_install_trend,-1) as cate7015_001_25_34_install_trend
,COALESCE(cate7015_004_18_install_trend,-1) as cate7015_004_18_install_trend
,COALESCE(cate7015_005_install_trend,-1) as cate7015_005_install_trend
,COALESCE(cate7015_006_55_install_trend,-1) as cate7015_006_55_install_trend
,COALESCE(cate7015_007_35_44_install_trend,-1) as cate7015_007_35_44_install_trend
,COALESCE(cate7015_010_18_install_trend,-1) as cate7015_010_18_install_trend
,COALESCE(cate7015_015_25_34_install_trend,-1) as cate7015_015_25_34_install_trend
,COALESCE(cate7015_016_18_install_trend,-1) as cate7015_016_18_install_trend
,COALESCE(cate7015_019_25_34_install_trend,-1) as cate7015_019_25_34_install_trend
,COALESCE(cate7015_020_18_install_trend,-1) as cate7015_020_18_install_trend
,COALESCE(cate7015_021_18_install_trend,-1) as cate7015_021_18_install_trend
,COALESCE(cate7016_003_18_24_install_trend,-1) as cate7016_003_18_24_install_trend
,COALESCE(cate7016_004_18_24_install_trend,-1) as cate7016_004_18_24_install_trend
,COALESCE(cate7016_004_55_install_trend,-1) as cate7016_004_55_install_trend
,COALESCE(cate7017_001_55_install_trend,-1) as cate7017_001_55_install_trend
,COALESCE(cate7017_002_55_install_trend,-1) as cate7017_002_55_install_trend
,COALESCE(cate7018_003_18_24_install_trend,-1) as cate7018_003_18_24_install_trend
,COALESCE(cate7019_101_18_24_install_trend,-1) as cate7019_101_18_24_install_trend
,COALESCE(cate7019_102_25_34_install_trend,-1) as cate7019_102_25_34_install_trend
,COALESCE(cate7019_107_45_54_install_trend,-1) as cate7019_107_45_54_install_trend
,COALESCE(cate7019_109_18_install_trend,-1) as cate7019_109_18_install_trend
,COALESCE(cate7019_110_18_24_install_trend,-1) as cate7019_110_18_24_install_trend
,COALESCE(cate7019_111_18_24_install_trend,-1) as cate7019_111_18_24_install_trend
,COALESCE(cate7019_114_18_install_trend,-1) as cate7019_114_18_install_trend
,COALESCE(cate7019_115_18_install_trend,-1) as cate7019_115_18_install_trend
,COALESCE(cate7019_116_18_24_install_trend,-1) as cate7019_116_18_24_install_trend
,COALESCE(cate7019_116_25_34_install_trend,-1) as cate7019_116_25_34_install_trend
,COALESCE(cate7019_117_18_24_install_trend,-1) as cate7019_117_18_24_install_trend
,COALESCE(cate7019_118_18_install_trend,-1) as cate7019_118_18_install_trend
,COALESCE(cate7019_119_18_install_trend,-1) as cate7019_119_18_install_trend
,COALESCE(cate7019_119_25_34_install_trend,-1) as cate7019_119_25_34_install_trend
,COALESCE(cate7019_120_18_install_trend,-1) as cate7019_120_18_install_trend
,COALESCE(cate7019_121_55_install_trend,-1) as cate7019_121_55_install_trend
,COALESCE(cate7019_121_install_trend,-1) as cate7019_121_install_trend
,COALESCE(cate7019_124_18_24_install_trend,-1) as cate7019_124_18_24_install_trend
,COALESCE(cate7019_126_18_install_trend,-1) as cate7019_126_18_install_trend
,COALESCE(cate7019_127_55_install_trend,-1) as cate7019_127_55_install_trend
,COALESCE(cate7019_128_18_24_install_trend,-1) as cate7019_128_18_24_install_trend
,COALESCE(cate7019_130_45_54_install_trend,-1) as cate7019_130_45_54_install_trend
,COALESCE(cate7019_132_18_24_install_trend,-1) as cate7019_132_18_24_install_trend
,COALESCE(cate7019_133_18_24_install_trend,-1) as cate7019_133_18_24_install_trend
,COALESCE(cate7019_135_45_54_install_trend,-1) as cate7019_135_45_54_install_trend
,COALESCE(cate7019_137_18_24_install_trend,-1) as cate7019_137_18_24_install_trend
,COALESCE(cate7019_139_45_54_install_trend,-1) as cate7019_139_45_54_install_trend
,COALESCE(cate8_35_44_install_trend,-1) as cate8_35_44_install_trend
,COALESCE(fin_25_55_install_trend,-1) as fin_25_55_install_trend
,COALESCE(fin_31_35_44_install_trend,-1) as fin_31_35_44_install_trend
,COALESCE(fin_31_55_install_trend,-1) as fin_31_55_install_trend
,COALESCE(fin_31_install_trend,-1) as fin_31_install_trend
,COALESCE(fin_34_55_install_trend,-1) as fin_34_55_install_trend
,COALESCE(fin_43_55_install_trend,-1) as fin_43_55_install_trend
,COALESCE(fin_50_18_install_trend,-1) as fin_50_18_install_trend
,COALESCE(fin_50_install_trend,-1) as fin_50_install_trend
,COALESCE(tgi1_18_4_18_24_install_trend,-1) as tgi1_18_4_18_24_install_trend
,COALESCE(tgi1_25_34_1_18_install_trend,-1) as tgi1_25_34_1_18_install_trend
,COALESCE(tgi1_25_34_2_install_trend,-1) as tgi1_25_34_2_install_trend
,COALESCE(tgi2_18_4_18_install_trend,-1) as tgi2_18_4_18_install_trend
,COALESCE(tgi2_35_44_2_55_install_trend,-1) as tgi2_35_44_2_55_install_trend
,COALESCE(tgi2_35_44_6_55_install_trend,-1) as tgi2_35_44_6_55_install_trend
,COALESCE(tgi2_45_54_2_18_24_install_trend,-1) as tgi2_45_54_2_18_24_install_trend
,COALESCE(tgi2_55_1_18_install_trend,-1) as tgi2_55_1_18_install_trend

,COALESCE(topic_0,-999) as topic_0
,COALESCE(topic_1,-999) as  topic_1
,COALESCE(topic_2,-999) as topic_2
,COALESCE(topic_3,-999) as topic_3
,COALESCE(topic_4,-999) as topic_4
,COALESCE(topic_5,-999) as topic_5
,COALESCE(topic_6,-999) as topic_6
,COALESCE(topic_7,-999) as topic_7
,COALESCE(topic_8,-999) as topic_8
,COALESCE(topic_9,-999) as topic_9
,COALESCE(topic_10,-999) as topic_10
,COALESCE(topic_11,-999) as topic_11
,COALESCE(topic_12,-999) as topic_12
,COALESCE(topic_13,-999) as topic_13
,COALESCE(topic_14,-999) as topic_14
,COALESCE(topic_15,-999) as topic_15
,COALESCE(topic_16,-999) as topic_16
,COALESCE(topic_17,-999) as topic_17
,COALESCE(topic_18,-999) as topic_18
,COALESCE(topic_19,-999) as topic_19

from(
    select *
    from $age_new_ratio_features_12m
    where day=${day}
)a 
left join(
    select *
    from $age_new_ratio_features_6m 
    where day=${day}
    
)b 
on a.device = b.device
left join(
    select *
    from $age_new_ratio_features_3m 
    where day=${day}
)c 
on a.device = c.device
left join(
    select *
    from $age_new_newIns_recency_features
    where day=${day}
)d 
on a.device = d.device
left join(
    select *
    from $age_new_Ins_recency_features
    where day=${day}
)e 
on a.device = e.device
left join(
    select *
    from $age_new_install_cnt_6mv12m
    where day=${day}
)f 
on a.device = f.device
left join(
    select *
    from $age_new_pkg_install_12
    where day=${day}
)g 
on a.device = g.device
left join(
    select *
    from $age_new_applist_install_bycate_id_whether
    where day=${day}
)h 
on a.device = h.device
left join(
    select *
    from $age_new_active_recency_features 
    where day=${day}
)i 
on a.device = i.device
left join(
    select *
    from $age_new_active_days_12
    where day=${day}
)j 
on a.device = j.device
left join(
    select *
    from $age_new_active_days_6
    where day=${day}
)k 
on a.device = k.device
left join(
    select *
    from $age_new_active_days_3
    where day=${day}

)l 
on a.device =l.device 
left join(
    select *
    from $age_new_active_days_1
    where day=${day}
)m 
on a.device = m.device
left join(
    select *
    from $age_new_embedding_cosin_bycate
    where day=${day}
)n 
on a.device = n.device
left join(
    select *
    from $age_new_topic_wgt
    where day=${day}
)p 
on a.device = p.device
"

hive -e "
alter table $age_new_age_features_all drop partition(day< $day_before_two_month);
"