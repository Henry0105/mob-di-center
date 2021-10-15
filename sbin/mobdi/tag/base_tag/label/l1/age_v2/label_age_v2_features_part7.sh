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
appdb="$rp_mobdi_report"
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

#label_device_pkg_install_uninstall_year_info_mf=rp_mobdi_report.label_device_pkg_install_uninstall_year_info_mf
install_uninstall_year_db=${label_device_pkg_install_uninstall_year_info_mf%.*}
install_uninstall_year_tb=${label_device_pkg_install_uninstall_year_info_mf#*.}

income_1001_university_bssid_index="${tmpdb}.income_1001_university_bssid_index"
income_1001_shopping_mall_bssid_index="${tmpdb}.income_1001_shopping_mall_bssid_index"
income_1001_traffic_bssid_index="${tmpdb}.income_1001_traffic_bssid_index"
income_1001_hotel_bssid_index="${tmpdb}.income_1001_hotel_bssid_index"
label_merge_all="${tmpdb}.model_merge_all_features"
#android_id_mapping_sec_df="dm_mobdi_mapping.android_id_mapping_sec_df"

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
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

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
        select device, pkg
        from $label_device_pkg_install_uninstall_year_info_mf
        where day=GET_LAST_PARTITION('$install_uninstall_year_db','$install_uninstall_year_tb','day')
        and refine_final_flag=-1
      ) t1
      left join
      (
        select * from dim_sdk_mapping.app_pkg_mapping_par where version='1000'
      ) t2
      on t1.pkg=t2.pkg
      group by t1.device, coalesce(t2.apppkg, t1.pkg)
    )t3
    left join (select * from $dim_app_pkg_mapping_par where version='1000') t4
    on t3.apppkg=t4.pkg
    group by t3.device,coalesce(t4.apppkg, t3.apppkg)
  ) a
  join
  (
    select apppkg, index from $mapping_age_app_index0
    where version='1000' and apppkg not in ('com.xwtec.sd.mobileclient','com.hanweb.android.sdzwfw.activity','com.inspur.vista.labor','com.android.clock.sd','com.qdccb.bank','com.sdhs.easy.high.road')
  ) b
  on a.apppkg=b.apppkg
)

insert overwrite table ${output_table_7} partition(day='${day}')
select seed.device,
      if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
      if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from seed
left join
(
  select device,index,cnt from age_uninstall_1y t2
)t
on seed.device = t.device
group by seed.device;
"



#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table_7} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_7} drop if exists partition($old_version)"
done
