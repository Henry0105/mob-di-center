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

output_table=${tmpdb}.tmp_score_part2
output_table_v3=${tmpdb}.tmp_score_part2_v3
:<<!
hive -v -e "
--catel1,55-73
--放在label_apppkg_feature_category_index.sh里面
--9 cate l2 --74-287
"
!
:<<!
hive -v -e "
CREATE TABLE mobdi_test.label_age_app_unstall_1y(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc ;


CREATE TABLE mobdi_test.label_age_pre_app_tgi_feature(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc ;

CREATE TABLE mobdi_test.label_age_pre_app_tgi_feature(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc ;

CREATE TABLE mobdi_test.label_age_applist_part2_beforechi(
  device string,
  index array<int>,
  cnt array<double>,
  tag int)
partitioned by (day string)
stored as orc ;
"
!

##-----part2
{
hive -v -e "
with seed as
(
  select *
  from $device_applist_new
  where day = '$day'
)
insert overwrite table ${output_table} partition(day='${day}')
select x.device
      ,if(y.device is null,array(0), y.index) index
      ,if(y.device is null,array(0.0), y.cnt) cnt
from
(
select device from seed group by device
)x
left join
(
  select device,collect_list(index) index,collect_list(cnt) cnt
  from
  (select a.device
     , b.index ,1.0 cnt
    from
    seed a
    join
    (
      select apppkg, index_after_chi index from $mapping_app_index where version='1000'
    ) b
    on a.pkg=b.apppkg
  )c group by device
)y
on x.device=y.device;
"
}

##-----part_age_applist_part2_v3
{
hive -v -e "
with seed as (
  select device,pkg from $device_applist_new where day='$day'
)
insert overwrite table ${tmpdb}.tmp_score_part2_v3 partition(day='${day}')
select x.*
from
(
  select device,
         if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
         if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
  from (
     select a.device
           , b.index ,1.0 cnt
      from  seed  a
      join
      (
      select apppkg, index from $age_app_index0_mapping where version='1000'
      ) b
      on a.pkg=b.apppkg
  )t
  where index not in (67,473,378,238,783,379)
  group by device
)x
"

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
}
