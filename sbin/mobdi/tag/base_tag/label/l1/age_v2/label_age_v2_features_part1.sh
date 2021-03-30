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

output_table=${tmpdb}.tmp_score_part1
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

##-----part1
{
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table ${output_table} partition(day='${day}')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device
      ,case
        when city_level_1001 = 1 then 0
        when city_level_1001 = 2  then 1
        when city_level_1001 = 3  then 2
        when city_level_1001 = 4  then 3
        when city_level_1001 = 5  then 4
        when city_level_1001 = 6  then 5
        else 6 end as index
       ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device
      ,case
        when factory in ('HUAWEI','HONOR') then 7
        when factory = 'OPPO' then 8
        when factory = 'VIVO' then 9
        when factory = 'XIAOMI' then 10
        when factory = 'SAMSUNG' then 11
        when factory = 'MEIZU' then 12
        when factory = 'ONEPLUS' then 13
        when factory = 'SMARTISAN' then 14
        when factory = 'GIONEE' then 15
        when factory = 'MEITU' then 16
        when factory = 'LEMOBILE' then 17
        when factory = '360' then 18
        when factory = 'BLACKSHARK' then 19
        else 20 end as index
      ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device
      ,case
        when split(sysver, '\\\\.')[0] = 10 then 21
        when split(sysver, '\\\\.')[0] = 9 then 22
        when split(sysver, '\\\\.')[0] = 8 then 23
        when split(sysver, '\\\\.')[0] = 7 then 24
        when split(sysver, '\\\\.')[0] = 6 then 25
        when split(sysver, '\\\\.')[0] = 5 then 26
        when split(sysver, '\\\\.')[0] <= 4 then 27
        when sysver = 'unknown' then 28
        else 29 end as index
      ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
    when diff_month < 12 then 30
    when diff_month >= 12 and diff_month < 24 then 31
    when diff_month >= 24 and diff_month < 36 then 32
    when diff_month >= 36 and diff_month < 48 then 33
    when diff_month >= 48 then 34
  else 35
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
  when tot_install_apps <= 10 then 36
  when tot_install_apps <= 20 then 37
  when tot_install_apps <= 30 then 38
  when tot_install_apps <= 50 then 39
  when tot_install_apps <= 100 then 40
  else 41
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all
  select device,
  case
  when price > 0 and price < 1000 then 42
  when price >= 1000 and price < 1499 then 43
  when price >= 1499 and price < 2399 then 44
  when price >= 2399 and price < 4000 then 45
  when price >= 4000 then 46
  else 47
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
    when house_price >= 0 and house_price < 8000 then 48
    when house_price >= 8000 and house_price < 12000 then 49
    when house_price >= 12000 and house_price < 22000 then 50
    when house_price >= 22000 and house_price < 40000 then 51
    when house_price >= 40000 and house_price < 60000 then 52
    when house_price >= 60000 then 53
    else 54
    end as index,
    1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.age.cate_l1'

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.age.cate_l2'


)t group by device
"
#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done
}
