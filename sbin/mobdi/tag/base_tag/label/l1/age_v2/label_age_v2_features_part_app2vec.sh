#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1
tmpdb=${dw_mobdi_md}
appdb="rp_mobdi_report"
#input
device_applist_new=${dim_device_applist_new_di}

#mapping
mapping_app_cate_index1="dm_sdk_mapping.mapping_age_cate_index1"
mapping_app_cate_index2="dm_sdk_mapping.mapping_age_cate_index2"
mapping_app_index="dm_sdk_mapping.mapping_age_app_index"
mapping_phonenum_year="dm_sdk_mapping.mapping_phonenum_year"
gdpoi_explode_big="dm_sdk_mapping.mapping_gdpoi_explode_big"
mapping_contacts_words_20000="dm_sdk_mapping.mapping_contacts_words_20000"
mapping_word_index="dm_sdk_mapping.mapping_age_word_index"
mapping_contacts_word2vec2="dm_sdk_mapping.mapping_contacts_word2vec2_view"

app_pkg_mapping="dm_sdk_mapping.app_pkg_mapping_par"
age_app_index0_mapping="dm_sdk_mapping.mapping_age_app_index0"

#tmp
label_phone_year="${appdb}.label_phone_year"
label_bssid_num="${appdb}.label_bssid_num"
label_distance_avg="${appdb}.label_distance_avg"
label_distance_night="${appdb}.label_distance_night"
label_homeworkdist="${appdb}.label_homeworkdist"
label_home_poiaround="${appdb}.label_home_poiaround"
label_work_poiaround="${appdb}.label_work_poiaround"
income_1001_university_bssid_index="${tmpdb}.income_1001_university_bssid_index"
income_1001_shopping_mall_bssid_index="${tmpdb}.income_1001_shopping_mall_bssid_index"
income_1001_traffic_bssid_index="${tmpdb}.income_1001_traffic_bssid_index"
income_1001_hotel_bssid_index="${tmpdb}.income_1001_hotel_bssid_index"
label_contact_words_chi="${appdb}.label_contact_words_chi"
label_contact_word2vec="${appdb}.label_contact_word2vec"
label_score_applist="${appdb}.label_score_applist"
label_app2vec="${appdb}.label_app2vec"

label_merge_all="${tmpdb}.model_merge_all_features"
label_apppkg_feature_index="${appdb}.label_l1_apppkg_feature_index"
label_apppkg_category_index="${appdb}.label_l1_apppkg_category_index"


#output
tmp_score_part1="${tmpdb}.tmp_score_part1"
tmp_score_part3="${tmpdb}.tmp_score_part3"
tmp_score_part4="${tmpdb}.tmp_score_part4"
tmp_score_part5="${tmpdb}.tmp_score_part5"
tmp_score_part6="${tmpdb}.tmp_score_part6"
tmp_score_part2="${tmpdb}.tmp_score_part2"
tmp_score_app2vec="${tmpdb}.tmp_score_app2vec"

tmp_score_part2_v3="${tmpdb}.tmp_score_part2_v3"
tmp_score_part5_v3="${tmpdb}.tmp_score_part5_v3"
tmp_score_part6_v3="${tmpdb}.tmp_score_part6_v3"
tmp_score_app2vec_v3="${tmpdb}.tmp_score_app2vec_v3"
tmp_score_part7="${tmpdb}.tmp_score_part7"
tmp_score_part8="${tmpdb}.tmp_score_part8"

output_table=${tmpdb}.tmp_score_app2vec
output_table_v3=${tmpdb}.tmp_score_app2vec_v3
:<<!
hive -v -e "
--catel1,55-73
--放在label_apppkg_feature_category_index.sh里面
--9 cate l2 --74-287
"
!

##v2的app2vec
{
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;

with seed as
(
  select *
  from $device_applist_new
  where day = '$day'
)
insert overwrite table ${output_table} partition(day='${day}')
select device,
avg(d1) as d1,avg(d2) as d2,avg(d3) as d3,avg(d4) as d4,avg(d5) as d5,avg(d6) as d6,avg(d7) as d7,avg(d8) as d8,avg(d9) as d9,
avg(d10) as d10,avg(d11) as d11,avg(d12) as d12,avg(d13) as d13,avg(d14) as d14,avg(d15) as d15,avg(d16) as d16,avg(d17) as d17,
avg(d18) as d18,avg(d19) as d19,avg(d20) as d20,avg(d21) as d21,avg(d22) as d22,avg(d23) as d23,avg(d24) as d24,avg(d25) as d25,
avg(d26) as d26,avg(d27) as d27,avg(d28) as d28,avg(d29) as d29,avg(d30) as d30,avg(d31) as d31,avg(d32) as d32,avg(d33) as d33,
avg(d34) as d34,avg(d35) as d35,avg(d36) as d36,avg(d37) as d37,avg(d38) as d38,avg(d39) as d39,avg(d40) as d40,avg(d41) as d41,
avg(d42) as d42,avg(d43) as d43,avg(d44) as d44,avg(d45) as d45,avg(d46) as d46,avg(d47) as d47,avg(d48) as d48,avg(d49) as d49,
avg(d50) as d50,avg(d51) as d51,avg(d52) as d52,avg(d53) as d53,avg(d54) as d54,avg(d55) as d55,avg(d56) as d56,avg(d57) as d57,
avg(d58) as d58,avg(d59) as d59,avg(d60) as d60,avg(d61) as d61,avg(d62) as d62,avg(d63) as d63,avg(d64) as d64,avg(d65) as d65,
avg(d66) as d66,avg(d67) as d67,avg(d68) as d68,avg(d69) as d69,avg(d70) as d70,avg(d71) as d71,avg(d72) as d72,avg(d73) as d73,
avg(d74) as d74,avg(d75) as d75,avg(d76) as d76,avg(d77) as d77,avg(d78) as d78,avg(d79) as d79,avg(d80) as d80,avg(d81) as d81,
avg(d82) as d82,avg(d83) as d83,avg(d84) as d84,avg(d85) as d85,avg(d86) as d86,avg(d87) as d87,avg(d88) as d88,avg(d89) as d89,
avg(d90) as d90,avg(d91) as d91,avg(d92) as d92,avg(d93) as d93,avg(d94) as d94,avg(d95) as d95,avg(d96) as d96,avg(d97) as d97,
avg(d98) as d98,avg(d99) as d99,avg(d100) as d100
from
seed  x
left join
  (select * from rp_mobdi_app.apppkg_app2vec_par_wi where day=GET_LAST_PARTITION('rp_mobdi_app', 'apppkg_app2vec_par_wi', 'day')) y
on x.pkg = y.apppkg
group by device;
"
} &

##-----v3的app2vec part_app2vec
{
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;


with seed as
(
  select *
  from $device_applist_new
  where day = '$day'
)
insert overwrite table ${output_table_v3} partition(day='${day}')
select device,
avg(d1) as d1,avg(d2) as d2,avg(d3) as d3,avg(d4) as d4,avg(d5) as d5,avg(d6) as d6,avg(d7) as d7,avg(d8) as d8,avg(d9) as d9,
avg(d10) as d10,avg(d11) as d11,avg(d12) as d12,avg(d13) as d13,avg(d14) as d14,avg(d15) as d15,avg(d16) as d16,avg(d17) as d17,
avg(d18) as d18,avg(d19) as d19,avg(d20) as d20,avg(d21) as d21,avg(d22) as d22,avg(d23) as d23,avg(d24) as d24,avg(d25) as d25,
avg(d26) as d26,avg(d27) as d27,avg(d28) as d28,avg(d29) as d29,avg(d30) as d30,avg(d31) as d31,avg(d32) as d32,avg(d33) as d33,
avg(d34) as d34,avg(d35) as d35,avg(d36) as d36,avg(d37) as d37,avg(d38) as d38,avg(d39) as d39,avg(d40) as d40,avg(d41) as d41,
avg(d42) as d42,avg(d43) as d43,avg(d44) as d44,avg(d45) as d45,avg(d46) as d46,avg(d47) as d47,avg(d48) as d48,avg(d49) as d49,
avg(d50) as d50,avg(d51) as d51,avg(d52) as d52,avg(d53) as d53,avg(d54) as d54,avg(d55) as d55,avg(d56) as d56,avg(d57) as d57,
avg(d58) as d58,avg(d59) as d59,avg(d60) as d60,avg(d61) as d61,avg(d62) as d62,avg(d63) as d63,avg(d64) as d64,avg(d65) as d65,
avg(d66) as d66,avg(d67) as d67,avg(d68) as d68,avg(d69) as d69,avg(d70) as d70,avg(d71) as d71,avg(d72) as d72,avg(d73) as d73,
avg(d74) as d74,avg(d75) as d75,avg(d76) as d76,avg(d77) as d77,avg(d78) as d78,avg(d79) as d79,avg(d80) as d80,avg(d81) as d81,
avg(d82) as d82,avg(d83) as d83,avg(d84) as d84,avg(d85) as d85,avg(d86) as d86,avg(d87) as d87,avg(d88) as d88,avg(d89) as d89,
avg(d90) as d90,avg(d91) as d91,avg(d92) as d92,avg(d93) as d93,avg(d94) as d94,avg(d95) as d95,avg(d96) as d96,avg(d97) as d97,
avg(d98) as d98,avg(d99) as d99,avg(d100) as d100
from
seed  x
left join
  (select * from rp_mobdi_app.apppkg_app2vec_par_wi where day='20201108') y
on x.pkg = y.apppkg
where x.pkg not in ('com.xwtec.sd.mobileclient','com.hanweb.android.sdzwfw.activity','com.inspur.vista.labor','com.android.clock.sd','com.qdccb.bank','com.sdhs.easy.high.road')
group by device;
"
} &

wait

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done

for old_version in `hive -e "show partitions ${output_table_v3} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_v3} drop if exists partition($old_version)"
done