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

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=${dw_mobdi_md}
appdb="rp_mobdi_report"
#input
device_applist_new=${dim_device_applist_new_di}

#mapping
#mapping_app_cate_index1="dim_sdk_mapping.mapping_age_cate_index1"
#mapping_app_cate_index2="dim_sdk_mapping.mapping_age_cate_index2"
#mapping_app_index="dim_sdk_mapping.mapping_age_app_index"
#mapping_phonenum_year="dim_sdk_mapping.mapping_phonenum_year"
#gdpoi_explode_big="dim_sdk_mapping.mapping_gdpoi_explode_big"
#mapping_contacts_words_20000="dim_sdk_mapping.mapping_contacts_words_20000"
#mapping_word_index="dim_sdk_mapping.mapping_age_word_index"
#mapping_contacts_word2vec2="dim_sdk_mapping.mapping_contacts_word2vec2_view"

#app_pkg_mapping="dim_sdk_mapping.app_pkg_mapping_par"
#age_app_index0_mapping="dim_sdk_mapping.mapping_age_app_index0"

#tmp
#label_phone_year="${appdb}.label_phone_year"
#label_bssid_num="${appdb}.label_bssid_num"
#label_distance_avg="${appdb}.label_distance_avg"
#label_distance_night="${appdb}.label_distance_night"
#label_homeworkdist="${appdb}.label_homeworkdist"
#label_home_poiaround="${appdb}.label_home_poiaround"
#label_work_poiaround="${appdb}.label_work_poiaround"
#label_contact_words_chi="${appdb}.label_contact_words_chi"
#label_contact_word2vec="${appdb}.label_contact_word2vec"
#label_score_applist="${appdb}.label_score_applist"
#label_app2vec="${appdb}.label_app2vec"
#label_apppkg_feature_index="${appdb}.label_l1_apppkg_feature_index"
#label_apppkg_category_index="${appdb}.label_l1_apppkg_category_index"

income_1001_university_bssid_index="${tmpdb}.income_1001_university_bssid_index"
income_1001_shopping_mall_bssid_index="${tmpdb}.income_1001_shopping_mall_bssid_index"
income_1001_traffic_bssid_index="${tmpdb}.income_1001_traffic_bssid_index"
income_1001_hotel_bssid_index="${tmpdb}.income_1001_hotel_bssid_index"
label_merge_all="${tmpdb}.model_merge_all_features"

#android_id_mapping_sec_df="dim_mobdi_mapping.android_id_mapping_sec_df"
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

## 结果临时表
output_table_7=${tmpdb}.tmp_score_part7
output_table_8=${tmpdb}.tmp_score_part8

##-----part_age_unstall_feature-part7
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
with seed as (
  select device,pkg from $device_applist_new where day='$day'
),
age_pre_app_tgi_feature_union as (
select t3.device, concat(t4.tag,':',tgi_level) index, count(*) cnt
from
(
    select t1.device, t2.apppkg
    from
    (
      select a.device
           , b.index ,1.0 cnt
      from  seed  a
      join
      (
      select apppkg, index from $mapping_age_app_index0 where version='1000'
      ) b
      on a.pkg=b.apppkg
    ) t1
    join
    (
    select apppkg, index from $mapping_age_app_index0 where version='1000'
    ) t2
    on t1.index=t2.index
)t3
join $mapping_age_app_tgi_level t4
on t3.apppkg=t4.apppkg
where t4.apppkg not in ('com.xwtec.sd.mobileclient','com.hanweb.android.sdzwfw.activity','com.inspur.vista.labor','com.android.clock.sd','com.qdccb.bank','com.sdhs.easy.high.road')
group by t3.device, t4.tag, tgi_level
),
age_pre_app_tgi_feature_final as
(
  select device,
  if(index is null,array(0),index) as index,
       if(cnt is null,array(0.0),cnt) as cnt
  from
  (
  select device,
         if(size(collect_list(t2.rk))=0,collect_set(0),collect_list(t2.rk)) as index,
         if(size(collect_list(t1.cnt))=0,collect_set(0.0),collect_list(cast (t1.cnt as double))) as cnt
  from age_pre_app_tgi_feature_union t1
  join $mapping_age_app_tgi_feature_index0 t2 -- 固定表
  on t1.index=t2.index
  group by t1.device
  )t
)
insert overwrite table ${output_table_8} partition(day='${day}')
select device,index,cnt from age_pre_app_tgi_feature_final;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table_8} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_8} drop if exists partition($old_version)"
done
