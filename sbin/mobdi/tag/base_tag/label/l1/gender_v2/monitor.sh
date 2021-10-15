#!/bin/bash
set -x -e

if [[ $# -lt 1 ]]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=$dm_mobdi_tmp
gender_feature_v2_final="$tmpdb.gender_feature_v2_final"
#gender_scoring_result_di="rp_mobdi_app.gender_scoring_result_di"
#gender_female_cl_full_min_max="dm_sdk_mapping.gender_probability_full_min_max"
#gender_breaks_max_min_by_full_final="dm_sdk_mapping.gender_breaks_max_min_by_full_final_new"

gender_incr_features_psi="$tmpdb.gender_incr_features_psi"
gender_incr_model_psi="$tmpdb.gender_incr_model_psi"

hive -e "
with gender_incr_score_features175_index as (
SELECT t1.device, t2.key feature, t2.value feature_value
FROM (select * from $gender_feature_v2_final where day='$day') t1
LATERAL VIEW explode (map(
'rt_cate_mf_rt_avg_cate17_male_female', rt_cate_mf_rt_avg_cate17_male_female,
'rt_cate_mf_rt_avg_cate33_male_female', rt_cate_mf_rt_avg_cate33_male_female,
'rt_cate_mf_rt_avg_cate34_male_female', rt_cate_mf_rt_avg_cate34_male_female,
'rt_cate_mf_rt_avg_cate35_male_female', rt_cate_mf_rt_avg_cate35_male_female,
'rt_cate_mf_rt_avg_cate36_male_female', rt_cate_mf_rt_avg_cate36_male_female,
'rt_cate_mf_rt_avg_cate37_male_female', rt_cate_mf_rt_avg_cate37_male_female,
'rt_cate_mf_rt_avg_cate5_male_female', rt_cate_mf_rt_avg_cate5_male_female,
'cnt_cate_l1_index15', cnt_cate_l1_index15,
 'cnt_cate_l1_index8', cnt_cate_l1_index8,
 'cnt_cate_l1_index19', cnt_cate_l1_index19,
 'cnt_cate_l1_index18', cnt_cate_l1_index18,
 'cnt_cate_l1_index12', cnt_cate_l1_index12,
 'cnt_cate_l1_index13', cnt_cate_l1_index13,
 'cnt_cate_l1_index5', cnt_cate_l1_index5,
 'cnt_cate_l1_index16', cnt_cate_l1_index16,
 'cnt_cate_l2_index102', cnt_cate_l2_index102,
 'cnt_cate_l2_index91', cnt_cate_l2_index91,
 'cnt_cate_l2_index10', cnt_cate_l2_index10,
 'cnt_cate_l2_index165', cnt_cate_l2_index165,
 'cnt_cate_l2_index214', cnt_cate_l2_index214,
 'cnt_cate_l2_index124', cnt_cate_l2_index124,
 'cnt_cate_l2_index162', cnt_cate_l2_index162,
 'cnt_cate_l2_index178', cnt_cate_l2_index178,
 'cnt_cate_l2_index140', cnt_cate_l2_index140,
 'cnt_cate_l2_index4', cnt_cate_l2_index4,
 'cnt_cate_l2_index196', cnt_cate_l2_index196,
 'cnt_cate_l2_index209', cnt_cate_l2_index209,
 'cnt_cate_l2_index96', cnt_cate_l2_index96,
 'cnt_cate_l2_index158', cnt_cate_l2_index158,
 'cnt_cate_l2_index41', cnt_cate_l2_index41,
 'cnt_cate_l2_index14', cnt_cate_l2_index14,
 'cnt_cate_l2_index20', cnt_cate_l2_index20,
 'cnt_cate_l2_index188', cnt_cate_l2_index188,
 'cnt_cate_l2_index86', cnt_cate_l2_index86,
 'cnt_cate_l2_index92', cnt_cate_l2_index92,
 'cnt_cate_l2_index201', cnt_cate_l2_index201,
 'cnt_cate_l2_index137', cnt_cate_l2_index137,
 'cnt_tgi5_tgi_male', cnt_tgi5_tgi_male,
 'cnt_tgi5_tgi_female', cnt_tgi5_tgi_female,
 'cnt_tgi5_tgi_female_high', cnt_tgi5_tgi_female_high,
 'cnt_tgi5_tgi_male_high', cnt_tgi5_tgi_male_high,
 'rt_cate_l1_index19', rt_cate_l1_index19,
 'rt_cate_l1_index11', rt_cate_l1_index11,
 'rt_cate_l1_index18', rt_cate_l1_index18,
 'rt_cate_l1_index12', rt_cate_l1_index12,
 'rt_cate_l1_index13', rt_cate_l1_index13,
 'rt_cate_l1_index5', rt_cate_l1_index5,
 'rt_cate_l2_index44', rt_cate_l2_index44,
 'rt_cate_l2_index102', rt_cate_l2_index102,
 'rt_cate_l2_index42', rt_cate_l2_index42,
 'rt_cate_l2_index156', rt_cate_l2_index156,
 'rt_cate_l2_index7', rt_cate_l2_index7,
 'rt_cate_l2_index57', rt_cate_l2_index57,
 'rt_cate_l2_index91', rt_cate_l2_index91,
 'rt_cate_l2_index10', rt_cate_l2_index10,
 'rt_cate_l2_index103', rt_cate_l2_index103,
 'rt_cate_l2_index115', rt_cate_l2_index115,
 'rt_cate_l2_index16', rt_cate_l2_index16,
 'rt_cate_l2_index52', rt_cate_l2_index52,
 'rt_cate_l2_index6', rt_cate_l2_index6,
 'rt_cate_l2_index214', rt_cate_l2_index214,
 'rt_cate_l2_index124', rt_cate_l2_index124,
 'rt_cate_l2_index152', rt_cate_l2_index152,
 'rt_cate_l2_index178', rt_cate_l2_index178,
 'rt_cate_l2_index140', rt_cate_l2_index140,
 'rt_cate_l2_index4', rt_cate_l2_index4,
 'rt_cate_l2_index71', rt_cate_l2_index71,
 'rt_cate_l2_index128', rt_cate_l2_index128,
 'rt_cate_l2_index45', rt_cate_l2_index45,
 'rt_cate_l2_index126', rt_cate_l2_index126,
 'rt_cate_l2_index196', rt_cate_l2_index196,
 'rt_cate_l2_index209', rt_cate_l2_index209,
 'rt_cate_l2_index94', rt_cate_l2_index94,
 'rt_cate_l2_index87', rt_cate_l2_index87,
 'rt_cate_l2_index41', rt_cate_l2_index41,
 'rt_cate_l2_index88', rt_cate_l2_index88,
 'rt_cate_l2_index14', rt_cate_l2_index14,
 'rt_cate_l2_index147', rt_cate_l2_index147,
 'rt_cate_l2_index93', rt_cate_l2_index93,
 'rt_cate_l2_index60', rt_cate_l2_index60,
 'rt_cate_l2_index188', rt_cate_l2_index188,
 'rt_cate_l2_index86', rt_cate_l2_index86,
 'rt_cate_l2_index59', rt_cate_l2_index59,
 'rt_cate_l2_index111', rt_cate_l2_index111,
 'rt_cate_l2_index92', rt_cate_l2_index92,
 'rt_cate_l2_index65', rt_cate_l2_index65,
 'rt_cate_l2_index201', rt_cate_l2_index201,
 'rt_cate_l2_index137', rt_cate_l2_index137,
 'rt_cate_l2_index159', rt_cate_l2_index159,
 'rt_tgi_male_female', rt_tgi_male_female,
 'rt_avg_tgi', rt_avg_tgi,
 'cnt_top_app56', cnt_top_app56,
 'cnt_top_app81', cnt_top_app81,
 'cnt_top_app121', cnt_top_app121,
 'cnt_top_app187', cnt_top_app187,
 'cnt_top_app247', cnt_top_app247,
 'cnt_top_app287', cnt_top_app287,
 'cnt_top_app180', cnt_top_app180,
 'cnt_top_app33', cnt_top_app33,
 'cnt_top_app38', cnt_top_app38,
 'cnt_top_app40', cnt_top_app40,
 'cnt_top_app43', cnt_top_app43,
 'cnt_top_app308', cnt_top_app308,
 'cnt_top_app234', cnt_top_app234,
 'cnt_top_app120', cnt_top_app120,
 'cnt_top_app386', cnt_top_app386,
 'cnt_top_app350', cnt_top_app350,
 'cnt_top_app61', cnt_top_app61,
 'rt_cate_mf_rt_avg_cate4_avg_tgi', rt_cate_mf_rt_avg_cate4_avg_tgi,
 'rt_cate_mf_rt_avg_cate40_avg_tgi', rt_cate_mf_rt_avg_cate40_avg_tgi,
 'rt_cate_mf_rt_avg_cate16_avg_tgi', rt_cate_mf_rt_avg_cate16_avg_tgi,
 'rt_cate_mf_rt_avg_cate2_avg_tgi', rt_cate_mf_rt_avg_cate2_avg_tgi,
 'rt_cate_mf_rt_avg_cate19_avg_tgi', rt_cate_mf_rt_avg_cate19_avg_tgi,
 'rt_cate_mf_rt_avg_cate34_avg_tgi', rt_cate_mf_rt_avg_cate34_avg_tgi,
 'rt_cate_mf_rt_avg_cate3_avg_tgi', rt_cate_mf_rt_avg_cate3_avg_tgi,
 'rt_cate_mf_rt_avg_cate12_avg_tgi', rt_cate_mf_rt_avg_cate12_avg_tgi,
 'rt_cate_mf_rt_avg_cate44_avg_tgi', rt_cate_mf_rt_avg_cate44_avg_tgi,
 'rt_cate_mf_rt_avg_cate21_avg_tgi', rt_cate_mf_rt_avg_cate21_avg_tgi,
 'rt_cate_mf_rt_avg_cate17_avg_tgi', rt_cate_mf_rt_avg_cate17_avg_tgi,
 'rt_cate_mf_rt_avg_cate38_avg_tgi', rt_cate_mf_rt_avg_cate38_avg_tgi,
 'rt_cate_mf_rt_avg_cate6_avg_tgi', rt_cate_mf_rt_avg_cate6_avg_tgi,
 'rt_cate_mf_rt_avg_cate15_avg_tgi', rt_cate_mf_rt_avg_cate15_avg_tgi,
 'rt_cate_mf_rt_avg_cate11_avg_tgi', rt_cate_mf_rt_avg_cate11_avg_tgi,
 'rt_cate_mf_rt_avg_cate8_avg_tgi', rt_cate_mf_rt_avg_cate8_avg_tgi,
 'rt_cate_mf_rt_avg_cate29_avg_tgi', rt_cate_mf_rt_avg_cate29_avg_tgi,
 'rt_cate_mf_rt_avg_cate3_male_female', rt_cate_mf_rt_avg_cate3_male_female,
 'rt_tgi5_tgi_male', rt_tgi5_tgi_male,
 'rt_tgi5_tgi_female', rt_tgi5_tgi_female,
 'rt_tgi5_tgi_female_high', rt_tgi5_tgi_female_high,
 'rt_tgi5_tgi_male_high', rt_tgi5_tgi_male_high,
 'rt_tgi5_tgi_normal', rt_tgi5_tgi_normal,
 'cnt_cate_l2_union_mf_app_cnt_edu', cnt_cate_l2_union_mf_app_cnt_edu,
 'cnt_cate_l2_union_mf_app_cnt_female_high', cnt_cate_l2_union_mf_app_cnt_female_high,
 'cnt_cate_l2_union_mf_cate_cnt_male_cnt', cnt_cate_l2_union_mf_cate_cnt_male_cnt,
 'cnt_cate_l2_union_mf_cate_cnt_tgi_male_female', cnt_cate_l2_union_mf_cate_cnt_tgi_male_female,
 'cnt_cate_l2_union_mf_cate_cnt_game', cnt_cate_l2_union_mf_cate_cnt_game,
 'cnt_cate_l2_union_mf_cate_cnt_female_cnt', cnt_cate_l2_union_mf_cate_cnt_female_cnt,
 'rt_cos_sim_tgi_male', rt_cos_sim_tgi_male,
 'rt_cos_sim_tgi_female_high', rt_cos_sim_tgi_female_high,
 'rt_cos_sim_tgi_male_high', rt_cos_sim_tgi_male_high,
 'rt_cos_sim_tgi_female', rt_cos_sim_tgi_female,
 'rt_cate_l1_cos_sim_index8', rt_cate_l1_cos_sim_index8,
 'rt_cate_l1_cos_sim_index18_female', rt_cate_l1_cos_sim_index18_female,
 'rt_cate_l1_cos_sim_index11_female', rt_cate_l1_cos_sim_index11_female,
 'rt_cate_l1_cos_sim_index8_female', rt_cate_l1_cos_sim_index8_female,
 'rt_cate_l1_cos_sim_index7_female', rt_cate_l1_cos_sim_index7_female,
 'rt_cate_l1_cos_sim_index10', rt_cate_l1_cos_sim_index10,
 'rt_cate_l1_cos_sim_index9_female', rt_cate_l1_cos_sim_index9_female,
 'rt_cate_l1_cos_sim_index5_female', rt_cate_l1_cos_sim_index5_female,
 'rt_cate_l1_cos_sim_index12_female', rt_cate_l1_cos_sim_index12_female,
 'rt_cate_l1_cos_sim_index16', rt_cate_l1_cos_sim_index16,
 'rt_cate_l1_cos_sim_index19', rt_cate_l1_cos_sim_index19,
 'rt_cate_l1_cos_sim_index2_female', rt_cate_l1_cos_sim_index2_female,
 'rt_cate_l1_cos_sim_index12', rt_cate_l1_cos_sim_index12,
 'rt_cate_l1_cos_sim_index13_female', rt_cate_l1_cos_sim_index13_female,
 'rt_cate_l1_cos_sim_index19_female', rt_cate_l1_cos_sim_index19_female,
 'rt_cate_l1_cos_sim_index10_female', rt_cate_l1_cos_sim_index10_female,
 'rt_cate_l1_cos_sim_index15_female', rt_cate_l1_cos_sim_index15_female,
 'rt_cate_l2_cos_sim_index138_female', rt_cate_l2_cos_sim_index138_female,
 'rt_cate_l2_cos_sim_index126_female', rt_cate_l2_cos_sim_index126_female,
 'rt_cate_l2_cos_sim_index81', rt_cate_l2_cos_sim_index81,
 'rt_cate_l2_cos_sim_index102', rt_cate_l2_cos_sim_index102,
 'rt_cate_l2_cos_sim_index6', rt_cate_l2_cos_sim_index6,
 'rt_cate_l2_cos_sim_index15', rt_cate_l2_cos_sim_index15,
 'rt_cate_l2_cos_sim_index110_female', rt_cate_l2_cos_sim_index110_female,
 'rt_cate_l2_cos_sim_index165_female', rt_cate_l2_cos_sim_index165_female,
 'rt_cate_l2_cos_sim_index88_female', rt_cate_l2_cos_sim_index88_female,
 'rt_cate_l2_cos_sim_index88', rt_cate_l2_cos_sim_index88,
 'rt_cate_l2_cos_sim_index91_female', rt_cate_l2_cos_sim_index91_female,
 'rt_cate_l2_cos_sim_index150_female', rt_cate_l2_cos_sim_index150_female,
 'rt_cate_l2_cos_sim_index188_female', rt_cate_l2_cos_sim_index188_female,
 'rt_cate_l2_cos_sim_index59', rt_cate_l2_cos_sim_index59,
 'rt_cate_l2_cos_sim_index142_female', rt_cate_l2_cos_sim_index142_female,
 'rt_cate_l2_cos_sim_index19_female', rt_cate_l2_cos_sim_index19_female,
 'rt_cate_l2_cos_sim_index15_female', rt_cate_l2_cos_sim_index15_female,
 'rt_cate_l2_cos_sim_index141', rt_cate_l2_cos_sim_index141,
 'rt_cate_l2_cos_sim_index134', rt_cate_l2_cos_sim_index134,
 'rt_cate_l2_cos_sim_index80_female', rt_cate_l2_cos_sim_index80_female,
 'rt_cate_l2_cos_sim_index14_female', rt_cate_l2_cos_sim_index14_female,
 'rt_cate_l2_cos_sim_index96_female', rt_cate_l2_cos_sim_index96_female,
 'rt_cate_l2_cos_sim_index124', rt_cate_l2_cos_sim_index124,
 'rt_cate_l2_cos_sim_index178', rt_cate_l2_cos_sim_index178,
 'rt_cate_l2_cos_sim_index157_female', rt_cate_l2_cos_sim_index157_female,
 'rt_cate_l2_cos_sim_index4', rt_cate_l2_cos_sim_index4,
 'rt_cate_l2_cos_sim_index155_female', rt_cate_l2_cos_sim_index155_female,
 'rt_cate_l2_cos_sim_index126', rt_cate_l2_cos_sim_index126,
 'rt_cate_l2_cos_sim_index141_female', rt_cate_l2_cos_sim_index141_female,
 'rt_cate_l2_cos_sim_index66', rt_cate_l2_cos_sim_index66,
 'rt_cate_l2_cos_sim_index137_female', rt_cate_l2_cos_sim_index137_female,
 'rt_cate_l2_cos_sim_index41', rt_cate_l2_cos_sim_index41,
 'rt_cate_l2_cos_sim_index145', rt_cate_l2_cos_sim_index145,
 'rt_cate_l2_cos_sim_index66_female', rt_cate_l2_cos_sim_index66_female,
 'rt_cate_l2_cos_sim_index40', rt_cate_l2_cos_sim_index40,
 'rt_cate_l2_cos_sim_index140_female', rt_cate_l2_cos_sim_index140_female,
 'rt_topic11', rt_topic11,
 'rt_topic2', rt_topic2,
 'rt_topic17', rt_topic17,
 'rt_topic20', rt_topic20,
 'rt_topic16', rt_topic16,
 'rt_topic3', rt_topic3,
 'rt_topic19', rt_topic19,
 'rt_topic7', rt_topic7,
 'rt_topic14', rt_topic14,
 'rt_topic6', rt_topic6
)) t2 as key, value
)
insert overwrite table $gender_incr_features_psi partition(day=$day)
select t1.feature, sum((t1.per-t2.per)*ln(t1.per/t2.per)) psi from (
select feature, bin_index, cast(count(1) as double)/sum(count(1)) over(partition by feature) per
from
(
select t2.feature, t1.feature_value, t2.bin_min, t2.bin_max, t2.bin_index
from (select feature, feature_value from gender_incr_score_features175_index) t1
join $dim_gender_breaks_max_min_by_full_final t2
on t1.feature=t2.feature
where t1.feature_value>=t2.bin_min and t1.feature_value<t2.bin_max
) tt
group by feature, bin_index) t1
join (select bin_index, per, feature from $dim_gender_breaks_max_min_by_full_final
) t2
on t1.bin_index=t2.bin_index and t1.feature=t2.feature
group by t1.feature;
"

hive -e "
insert overwrite table $gender_incr_model_psi partition(day=$day)
select sum((t1.per-t2.per)*ln(t1.per/t2.per)) psi from (
select bin_index, cast(count(1) as double)/sum(count(1)) over() per
from (
select t1.*, t2.bin_min, t2.bin_max, bin_index,type
from (select device, gender, case when gender=0 then 1-probability else probability end probability
from $gender_scoring_result_di where day='$day') t1
join $dim_gender_female_cl_full_min_max t2
on 1=1
where t2.type=1 and t1.probability>=t2.bin_min and t1.probability<t2.bin_max
) tt
group by bin_index) t1
join (select bin_index, 0.1 per from $dim_gender_female_cl_full_min_max
where type=1) t2
on t1.bin_index=t2.bin_index
"