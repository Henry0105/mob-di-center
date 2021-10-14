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

tmp_income1001_part7_p1=${tmpdb}.tmp_income1001_part7_p1_${day}
output_table_7="${tmpdb}.tmp_income1001_part7"
output_table_8="${tmpdb}.tmp_income1001_part8"

##-----part_age_unstall_feature-part7
{
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;
drop table if exists ${tmp_income1001_part7_p1};
create table if not exists ${tmp_income1001_part7_p1} as
with seed as (
  select device from $device_applist_new where day='$day' group by device
),
age_uninstall_1y as (
  select a.device
       , b.index ,1.0 cnt
  from
  (
    select t3.device,coalesce(t4.apppkg, t3.apppkg) apppkg
    from
    (
      select t1.device, coalesce(t2.apppkg, t1.pkg) apppkg from
      (
        select seed.device,pkg from seed left join
        (
        select device, pkg
        from rp_mobdi_report.label_device_pkg_install_uninstall_year_info_mf
        where day=GET_LAST_PARTITION('rp_mobdi_report','label_device_pkg_install_uninstall_year_info_mf','day') and refine_final_flag=-1
        )mf  on seed.device=mf.device
      ) t1
      left join
      (
        select * from dm_sdk_mapping.app_pkg_mapping_par where version='1000'
      ) t2
      on t1.pkg=t2.pkg
      group by t1.device, coalesce(t2.apppkg, t1.pkg)
    )t3
    left join (select * from dm_sdk_mapping.app_pkg_mapping_par where version='1000') t4
    on t3.apppkg=t4.pkg
    group by t3.device,coalesce(t4.apppkg, t3.apppkg)
  ) a
  join
  (
    select apppkg, index from $age_app_index0_mapping 
  ) b
  on a.apppkg=b.apppkg
)
select device,
if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device,index,cnt from age_uninstall_1y t2
)t
group by device
"

HADOOP_USER_NAME=dba hive -v -e "
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;

with seed as (
  select device from $device_applist_new where day='$day' group by device
)
insert overwrite table ${output_table_7} partition(day='${day}')
select a.device,if(b.index is null,array(0),b.index) as index,
if(b.cnt is null,array(0.0),b.cnt) as cnt
from
seed a
left join ${tmp_income1001_part7_p1} b
on a.device=b.device
"
} &

##-----part_age_pre_app_tgi_feature-part8
{
HADOOP_USER_NAME=dba hive -v -e "
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
      select apppkg, index from $age_app_index0_mapping
      ) b
      on a.pkg=b.apppkg
    ) t1
    join
    (
    select apppkg, index from $age_app_index0_mapping
    ) t2
    on t1.index=t2.index
)t3
join dm_sdk_mapping.mapping_age_app_tgi_level t4
on t3.apppkg=t4.apppkg
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
  join dm_sdk_mapping.mapping_income1001_v2_app_tgi_index0 t2
  on t1.index=t2.index
  group by t1.device
  )t
)
insert overwrite table ${output_table_8} partition(day='${day}')
select device,index,cnt from age_pre_app_tgi_feature_final;
"
} &

wait

## 删除中间临时表
hive -v -e "drop table if exists ${tmp_income1001_part7_p1}"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table_7} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_7} drop if exists partition($old_version)"
done

for old_version in `hive -e "show partitions ${output_table_8} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_8} drop if exists partition($old_version)"
done
