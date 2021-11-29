#!/bin/bash

:
@owner:luost
@DESCribe:full表-分区统一清理
@projectName:MOBDI


set -e -x

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
p7_day=`date -d "$day -7 days" +%Y%m%d`
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")

source /home/dba/mobdi_center/conf/hive-env.sh

#database
tmpdb=$dm_mobdi_tmp

#定义函数
drop_partition(){
	table=$1
	day=$2
	echo "ALTER TABLE $table DROP PARTITION(day < $day);"
}

#agebin、agebin1001、agebin1002、agebin1003
tmp_score_part1=${tmpdb}.tmp_score_part1
tmp_score_part2=${tmpdb}.tmp_score_part2
tmp_score_part2_v3=${tmpdb}.tmp_score_part2_v3
tmp_score_part3=${tmpdb}.tmp_score_part3
tmp_score_part4=${tmpdb}.tmp_score_part4
tmp_score_part5=${tmpdb}.tmp_score_part5
tmp_score_part5_v3=${tmpdb}.tmp_score_part5_v3
tmp_score_part6=${tmpdb}.tmp_score_part6
tmp_score_part6_v3=${tmpdb}.tmp_score_part6_v3
tmp_score_part7=${tmpdb}.tmp_score_part7
tmp_score_part8=${tmpdb}.tmp_score_part8
tmp_part_app2vec=${tmpdb}.tmp_part_app2vec
tmp_score_app2vec=${tmpdb}.tmp_score_app2vec
tmp_part_app2vec_v3=${tmpdb}.tmp_part_app2vec_v3
tmp_score_app2vec_v3=${tmpdb}.tmp_score_app2vec_v3

hive -v -e "
`drop_partition $tmp_score_part1 $p7_day`
`drop_partition $tmp_score_part2 $p7_day`
`drop_partition $tmp_score_part2_v3 $p7_day`
`drop_partition $tmp_score_part3 $p7_day`
`drop_partition $tmp_score_part4 $p7_day`
`drop_partition $tmp_score_part5 $p7_day`
`drop_partition $tmp_score_part5_v3 $p7_day`
`drop_partition $tmp_score_part6_v3 $p7_day`
`drop_partition $tmp_score_part7 $p7_day`
`drop_partition $tmp_score_part8 $p7_day`
`drop_partition $tmp_part_app2vec $p7_day`
`drop_partition $tmp_score_app2vec $p7_day`
`drop_partition $tmp_part_app2vec_v3 $p7_day`
`drop_partition $tmp_score_app2vec_v3 $p7_day`
"

#car
tmp_car_score_part2=${tmpdb}.tmp_car_score_part2
tmp_car_score_part5=${tmpdb}.tmp_car_score_part5
tmp_car_score_part7=${tmpdb}.tmp_car_score_part7
tmp_car_score_part8=${tmpdb}.tmp_car_score_part8
tmp_part_car_app2vec=${tmpdb}.tmp_part_car_app2vec
tmp_car_pre_app2vec=${tmpdb}.tmp_car_pre_app2vec

hive -v -e "
`drop_partition $tmp_car_score_part2 $p7_day`
`drop_partition $tmp_car_score_part5 $p7_day`
`drop_partition $tmp_car_score_part7 $p7_day`
`drop_partition $tmp_car_score_part8 $p7_day`
`drop_partition $tmp_part_car_app2vec $p7_day`
`drop_partition $tmp_car_pre_app2vec $p7_day`
"

#edu
tmp_edu_score_part2=${tmpdb}.tmp_edu_score_part2
tmp_edu_score_part7=${tmpdb}.tmp_edu_score_part7
tmp_edu_score_part8=${tmpdb}.tmp_edu_score_part8

hive -v -e "
`drop_partition $tmp_edu_score_part2 $p7_day`
`drop_partition $tmp_edu_score_part7 $p7_day`
`drop_partition $tmp_edu_score_part8 $p7_day`
"

#gender
gender_feature_v2_part1=${tmpdb}.gender_feature_v2_part1
gender_feature_v2_part2=${tmpdb}.gender_feature_v2_part2
gender_feature_v2_part3=${tmpdb}.gender_feature_v2_part3
gender_feature_v2_part4=${tmpdb}.gender_feature_v2_part4
gender_feature_v2_part5=${tmpdb}.gender_feature_v2_part5
gender_feature_v2_part6=${tmpdb}.gender_feature_v2_part6
gender_feature_v2_part7=${tmpdb}.gender_feature_v2_part7
gender_feature_v2_part8=${tmpdb}.gender_feature_v2_part8
gender_feature_v2_part9=${tmpdb}.gender_feature_v2_part9
gender_feature_v2_part10=${tmpdb}.gender_feature_v2_part10
gender_feature_v2_part11=${tmpdb}.gender_feature_v2_part11
gender_feature_v2_part12=${tmpdb}.gender_feature_v2_part12
gender_feature_v2_part13=${tmpdb}.gender_feature_v2_part13
gender_feature_v2_part14=${tmpdb}.gender_feature_v2_part14
gender_feature_v2_part15=${tmpdb}.gender_feature_v2_part15
gender_feature_v2_part16=${tmpdb}.gender_feature_v2_part16
gender_app2vec_vec2_score_test_2k=${tmpdb}.gender_app2vec_vec2_score_test_2k
gender_feature_v2_part17=${tmpdb}.gender_feature_v2_part17
gender_device_cate_l2_app2vec_vec2_score=${tmpdb}.gender_device_cate_l2_app2vec_vec2_score
gender_feature_v2_part18=${tmpdb}.gender_feature_v2_part18
gender_app2vec_device_tgi_vec2_score=${tmpdb}.gender_app2vec_device_tgi_vec2_score
gender_feature_v2_part19=${tmpdb}.gender_feature_v2_part19
gender_feature_v2_final=${tmpdb}.gender_feature_v2_final

hive -v -e "
`drop_partition $gender_feature_v2_part1 $p7_day`
`drop_partition $gender_feature_v2_part2 $p7_day`
`drop_partition $gender_feature_v2_part3 $p7_day`
`drop_partition $gender_feature_v2_part4 $p7_day`
`drop_partition $gender_feature_v2_part5 $p7_day`
`drop_partition $gender_feature_v2_part6 $p7_day`
`drop_partition $gender_feature_v2_part7 $p7_day`
`drop_partition $gender_feature_v2_part8 $p7_day`
`drop_partition $gender_feature_v2_part9 $p7_day`
`drop_partition $gender_feature_v2_part10 $p7_day`
`drop_partition $gender_feature_v2_part11 $p7_day`
`drop_partition $gender_feature_v2_part12 $p7_day`
`drop_partition $gender_feature_v2_part13 $p7_day`
`drop_partition $gender_feature_v2_part14 $p7_day`
`drop_partition $gender_feature_v2_part15 $p7_day`
`drop_partition $gender_feature_v2_part16 $p7_day`
`drop_partition $gender_app2vec_vec2_score_test_2k $p7_day`
`drop_partition $gender_feature_v2_part17 $p7_day`
`drop_partition $gender_device_cate_l2_app2vec_vec2_score $p7_day`
`drop_partition $gender_feature_v2_part18 $p7_day`
`drop_partition $gender_app2vec_device_tgi_vec2_score $p7_day`
`drop_partition $gender_feature_v2_part19 $p7_day`
`drop_partition $gender_feature_v2_final $p7_day`
"

#income_1001_v2
tmp_income1001_part2=${tmpdb}.tmp_income1001_part2
tmp_income1001_part7=${tmpdb}.tmp_income1001_part7
tmp_income1001_part8=${tmpdb}.tmp_income1001_part8

hive -v -e "
`drop_partition $tmp_income1001_part2 $p7_day`
`drop_partition $tmp_income1001_part7 $p7_day`
`drop_partition $tmp_income1001_part8 $p7_day`
"

#occupation_1002
tmp_occ1002_predict_part2=${tmpdb}.tmp_occ1002_predict_part2
hive -v -e "
`drop_partition $tmp_occ1002_predict_part2 $p7_day`
"

#agebin1004
age_new_ratio_features_12m="${tmpdb}.age_new_ratio_features_12m"
age_new_ratio_features_6m="${tmpdb}.age_new_ratio_features_6m"
age_new_install_cnt_6mv12m="${tmpdb}.age_new_install_cnt_6mv12m"
age_new_ratio_features_3m="${tmpdb}.age_new_ratio_features_3m"
age_new_pkg_install_12="${tmpdb}.age_new_pkg_install_12"
age_new_newIns_recency_features="${tmpdb}.age_new_newIns_recency_features"
age_new_Ins_recency_features="${tmpdb}.age_new_Ins_recency_features"
age_new_active_recency_features="${tmpdb}.age_new_active_recency_features"
age_new_applist_install_bycate_id_whether="${tmpdb}.age_new_applist_install_bycate_id_whether"
age_new_active_days_12="${tmpdb}.age_new_active_days_12"
age_new_active_days_6="${tmpdb}.age_new_active_days_6"
age_new_active_days_3="${tmpdb}.age_new_active_days_3"
age_new_active_days_1="${tmpdb}.age_new_active_days_1"
age_new_uninstall_avg_embedding="${tmpdb}.age_new_uninstall_avg_embedding"
age_new_uninstall_avg_embedding_cosin_temp="${tmpdb}.age_new_uninstall_avg_embedding_cosin_temp"
age_new_embedding_cosin_bycate="${tmpdb}.age_new_embedding_cosin_bycate"
age_new_topic_wgt="${tmpdb}.age_new_topic_wgt"
age_new_age_features_all="${tmpdb}.age_new_features_all"

dd=`date -d "$day" +%d`
if [ $dd -eq 10 ];then
hive -v -e "
`drop_partition $age_new_ratio_features_12m $day_before_one_month`
`drop_partition $age_new_ratio_features_6m $day_before_one_month`
`drop_partition $age_new_install_cnt_6mv12m $day_before_one_month`
`drop_partition $age_new_ratio_features_3m $day_before_one_month`
`drop_partition $age_new_pkg_install_12 $day_before_one_month`
`drop_partition $age_new_newIns_recency_features $day_before_one_month`
`drop_partition $age_new_Ins_recency_features $day_before_one_month`
`drop_partition $age_new_active_recency_features $day_before_one_month`
`drop_partition $age_new_applist_install_bycate_id_whether $day_before_one_month`
`drop_partition $age_new_active_days_12 $day_before_one_month`
`drop_partition $age_new_active_days_6 $day_before_one_month`
`drop_partition $age_new_active_days_3 $day_before_one_month`
`drop_partition $age_new_active_days_1 $day_before_one_month`
`drop_partition $age_new_uninstall_avg_embedding $day_before_one_month`
`drop_partition $age_new_uninstall_avg_embedding_cosin_temp $day_before_one_month`
`drop_partition $age_new_embedding_cosin_bycate $day_before_one_month`
`drop_partition $age_new_topic_wgt $day_before_one_month`
`drop_partition $age_new_age_features_all $day_before_one_month`
"
fi
