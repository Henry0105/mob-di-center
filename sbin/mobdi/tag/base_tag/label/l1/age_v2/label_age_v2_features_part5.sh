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
mapping_app_cate_index1="dim_sdk_mapping.mapping_age_cate_index1"
mapping_app_cate_index2="dim_sdk_mapping.mapping_age_cate_index2"
mapping_app_index="dim_sdk_mapping.mapping_age_app_index"
mapping_phonenum_year="dim_sdk_mapping.mapping_phonenum_year"
gdpoi_explode_big="dim_sdk_mapping.mapping_gdpoi_explode_big"
mapping_contacts_words_20000="dim_sdk_mapping.mapping_contacts_words_20000"
mapping_word_index="dim_sdk_mapping.mapping_age_word_index"
mapping_contacts_word2vec2="dim_sdk_mapping.mapping_contacts_word2vec2_view"

app_pkg_mapping="dim_sdk_mapping.app_pkg_mapping_par"
age_app_index0_mapping="dim_sdk_mapping.mapping_age_app_index0"
android_id_mapping_sec_df="dm_mobdi_mapping.android_id_mapping_sec_df"
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


## 中间临时表
tmp_label_contact_words=${appdb}.label_contact_words_${day}
tmp_label_contact_words_chi=${appdb}.label_contact_words_chi_${day}


tmp_tables=($tmp_label_contact_words $tmp_label_contact_words_chi)

## 结果临时表
output_table=${tmpdb}.tmp_score_part5
output_table_v3=${tmpdb}.tmp_score_part5_v3

#id_mapping最新分区
full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_id_mapping_android_sec_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

##-----part5
{
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';

drop table if exists ${tmp_label_contact_words};
create table ${tmp_label_contact_words} stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select t1.device
      ,if(t2.device is null,array(0), t2.index) index
      ,if(t2.device is null,array(0.0), t2.cnt) cnt
from
seed t1
left join
(
select device,collect_list(index) index,collect_list(cnt) cnt
from
(select device,index,1.0 cnt
from
(select x.device,y.word_index
from
(
  select device,phone
  from
  (
    select *,row_number() over(partition by device order by pn_tm desc) rn
    from
    (
      select device,n.phone,n.pn_tm
      from
      (
        select a.device,concat(phone,'=',phone_ltm) phone_list
        from seed a
        join dm_mobdi_mapping.dim_id_mapping_android_df_view b
        on a.device=b.device
      )c lateral view explode_tags(phone_list) n as phone,pn_tm
    )d       where length(phone) = 11
  )e where rn=1
)x
join
(select * from $mapping_contacts_words_20000 where version='1000')y
on x.phone=y.phone
)xx
lateral view explode(word_index) n as index
)yy group by device
)t2 on t1.device=t2.device
;
"
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${tmp_label_contact_words_chi};
create table ${tmp_label_contact_words_chi} stored as orc as
select device,
if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(select a.device,
if(b.index_before_chi is null,null,b.index_after_chi) index,
if(b.index_before_chi is null,null,1.0) cnt
from
(select device,index_old
from ${tmp_label_contact_words}
lateral view explode(index) n as index_old
)a
left join (select * from $mapping_word_index where version='1000') b
on a.index_old = b.index_before_chi
)x group by device;

insert overwrite  table ${output_table} partition(day='${day}')
select device,index,cnt
from ${tmp_label_contact_words_chi}
;
"
} &

##v3版的part5通讯录特征
{
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)

insert overwrite table ${output_table_v3} partition(day='${day}')
select a.device,
       if(b.device is null,array(0), b.index) as index,
       if(b.device is null,array(0.0), b.cnt) as cnt
from seed a
left join
(
    select device,
           collect_list(index) as index,
           collect_list(cnt) as cnt
    from
    (
        select device,index,1.0 cnt
        from
        (
            select x.device,y.word_index
            from
            (
                select device,
                       pid
                from
                (
                    select *,row_number() over(partition by device order by pn_tm desc) rn
                    from
                    (
                        select d.device,d.pid,d.pn_tm
                        from
                        (
                            select device,
                                   n.pid,
                                   n.pn_tm
                            from
                            (
                                select a.device,
                                       concat(pid,'=',pid_ltm) as pid_list
                                from seed a
                                inner join
                                (
                                    select device,pid,pid_ltm
                                    from $android_id_mapping_sec_df
                                    where version = '$full_last_version'
                                ) b
                                on a.device = b.device
                            )c
                            lateral view explode_tags(pid_list) n as pid,pn_tm
                        )d
                        left join
                        (
                            select pid_id,country_code
                            from $dim_pid_attribute_full_par_secview
                        )e
                        on d.pid = e.pid_id
                        where e.country_code = 'cn'
                    )f
                )g
                where rn = 1
            )x
            inner join
            (
                select *
                from $mapping_contacts_words_20000_sec
                where version = '1000'
            )y
            on x.pid = y.pid
        )xx
        lateral view explode(word_index) n as index
    )yy
    group by device
)b
on a.device = b.device;
"
} &

wait

## 删除中间临时表
for tmp_table in ${tmp_tables[*]};
do
  hive -v -e "drop table if exists ${tmp_table}"
done


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