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
appdb=$rp_mobdi_report
#input
device_applist_new=${dim_device_applist_new_di}

tmp_anticheat_device_bssid_cnt_30days=$tmpdb.tmp_anticheat_device_bssid_cnt_30days
tmp_anticheat_device_avgdistance_pre=$tmpdb.tmp_anticheat_device_avgdistance_pre
tmp_anticheat_device_nightdistance_pre=$tmpdb.tmp_anticheat_device_nightdistance_pre
avgdistance_pre_db=${tmp_anticheat_device_avgdistance_pre%.*}
avgdistance_pre_tb=${tmp_anticheat_device_avgdistance_pre#*.}

nightdistance_pre_db=${tmp_anticheat_device_nightdistance_pre%.*}
nightdistance_pre_tb=${tmp_anticheat_device_nightdistance_pre#*.}

bssid_cnt_db=${tmp_anticheat_device_bssid_cnt_30days%.*}
bssid_cnt_tb=${tmp_anticheat_device_bssid_cnt_30days#*.}



#mapping
#mapping_app_cate_index1="dm_sdk_mapping.mapping_age_cate_index1"
#mapping_app_cate_index2="dm_sdk_mapping.mapping_age_cate_index2"
#mapping_app_index="dm_sdk_mapping.mapping_age_app_index"
#mapping_phonenum_year="dm_sdk_mapping.mapping_phonenum_year"
#gdpoi_explode_big="dm_sdk_mapping.mapping_gdpoi_explode_big"
#mapping_contacts_words_20000="dm_sdk_mapping.mapping_contacts_words_20000"
#mapping_word_index="dm_sdk_mapping.mapping_age_word_index"
#mapping_contacts_word2vec2="dm_sdk_mapping.mapping_contacts_word2vec2_view"

#app_pkg_mapping="dm_sdk_mapping.app_pkg_mapping_par"
#age_app_index0_mapping="dm_sdk_mapping.mapping_age_app_index0"

#android_id_mapping_sec_df="dim_mobdi_mapping.android_id_mapping_sec_df"

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
tmp_label_phone_year=${appdb}.label_phone_year_${day}
tmp_label_bssid_num=${appdb}.label_bssid_num_${day}
tmp_label_distance_avg=${appdb}.label_distance_avg_${day}
tmp_label_distance_night=${appdb}.label_distance_night_${day}
tmp_label_homeworkdist=${appdb}.label_homeworkdist_${day}
tmp_label_home_poiaround=${appdb}.label_home_poiaround_${day}
tmp_label_work_poiaround=${appdb}.label_work_poiaround_${day}
tmp_home_poi=${tmpdb}.tmp_home_poi_${day}
tmp_work_poi=${tmpdb}.tmp_work_poi_${day}

## 删中间临时表7天前数据 中间结果表其他脚本有用到
tmp_tables=(${appdb}.label_phone_year_${b7day} ${appdb}.label_bssid_num_${b7day} ${appdb}.label_distance_avg_${b7day} ${appdb}.label_distance_night_${b7day} ${appdb}.label_homeworkdist_${b7day} ${appdb}.label_home_poiaround_${b7day} ${appdb}.label_work_poiaround_${b7day} ${tmpdb}.tmp_home_poi_${b7day} ${tmpdb}.tmp_work_poi_${b7day})

## 结果临时表
output_table=${tmpdb}.tmp_score_part3

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

#id_mapping最新分区
full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dim_mobdi_mapping', 'android_id_mapping_sec_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

##-----part3

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${tmp_label_phone_year};
create table ${tmp_label_phone_year} stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)

select device,
       year
from
(
    select *,row_number() over(partition by device order by pn_tm desc) rn
    from
    (
        select d.device,d.pid,d.pn_tm,e.year
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
                    from $id_mapping_android_sec_df
                    where version = '$full_last_version'
                ) b
                on a.device = b.device
            )c
            lateral view explode_tags(pid_list) n as pid,pn_tm
        )d
        left join
        (
            select pid_id,year,country_code
            from $dim_pid_attribute_full_par_secview
        )e
        on d.pid = e.pid_id
        where e.country_code = 'cn'
    )f
)g
where rn = 1;
"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
create temporary function get_distance as 'com.youzu.mob.java.udf.GetDistance';

drop table if exists ${tmp_label_bssid_num};
create table ${tmp_label_bssid_num} stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select device,cnt
from
(select a.device,b.cnt,b.day,row_number() over(partition by a.device order by b.day desc) rn
from seed a
left join
(
select * from
$tmp_anticheat_device_bssid_cnt_30days
where day=GET_LAST_PARTITION('$bssid_cnt_db', '$bssid_cnt_tb', 'day')
)b
on a.device=b.device
)c where rn=1;

drop table if exists ${tmp_label_distance_avg};
create table ${tmp_label_distance_avg} stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select device,distance
from
(select a.device,b.distance,b.day,row_number() over(partition by a.device order by b.day desc) rn
from seed a
left join
(
  select *
  from $tmp_anticheat_device_avgdistance_pre
  where day=GET_LAST_PARTITION('$avgdistance_pre_db', '$avgdistance_pre_tb', 'day') and timewindow='30'
)b
on a.device=b.device
)c where rn=1
;

drop table if exists ${tmp_label_distance_night};
create  table ${tmp_label_distance_night} stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select device,distance
from
(select a.device,b.distance,b.day,row_number() over(partition by a.device order by b.day desc) rn
from seed a
left join
(
  select *
  from $tmp_anticheat_device_nightdistance_pre
  where day=GET_LAST_PARTITION('$nightdistance_pre_db', '$nightdistance_pre_tb', 'day') and timewindow='30'
)b
on a.device=b.device
)c where rn=1
;

drop table if exists ${tmp_label_homeworkdist};
create  table ${tmp_label_homeworkdist} stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select device,
lat_home,lon_home,lat_work,lon_work
      ,case when lat_home is null then null
            when lat_work is null then null
            else get_distance(lat_home,lon_home,lat_work,lon_work) end as home_work_dist
from
(
  select a.device
      ,if(b.residence like '%lat%',split(split(b.residence, ',')[0],':')[1],null) as lat_home
      ,if(b.residence like '%lat%',split(split(b.residence, ',')[1],':')[1],null) as lon_home
      ,if(b.workplace like '%lat%',split(split(b.workplace, ',')[0],':')[1],null) as lat_work
      ,if(b.workplace like '%lat%',split(split(b.workplace, ',')[1],':')[1],null) as lon_work
  from seed a
  join rp_mobdi_app.rp_device_profile_full_view b
  on a.device=b.device
)t;

"


spark2-submit \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 12G \
--executor-memory 35G \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_home lat,lon_home lon from ${tmp_label_homeworkdist} where lat_home is not null \",
    \"poiTable\": \"$dim_gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \"'01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20' \"
    },
    \"targetTable\": \"${tmp_home_poi} \"
}"

spark2-submit \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 12G \
--executor-memory 35G \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_work lat,lon_work lon from ${tmp_label_homeworkdist} where lat_work is not null \",
    \"poiTable\": \"$dim_gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \" '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20' \"
    },
    \"targetTable\": \"${tmp_work_poi} \"
}"


hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${tmp_label_home_poiaround};
create table ${tmp_label_home_poiaround} stored as orc as
select device,poi_type
from ${tmp_home_poi}
group by device,poi_type
;
drop table if exists ${tmp_label_work_poiaround};
create  table ${tmp_label_work_poiaround} stored as orc as
select device,poi_type
from ${tmp_work_poi}
group by device,poi_type
;
"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table ${output_table} partition(day='${day}')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device
        ,case when year>='1995' and year<='1999' then 0
            when year>='2000' and year<='2004' then 1
            when year>='2005' and year<='2009' then 2
            when year>='2010' and year<='2014' then 3
            when year>='2015' and year<='2019' then 4
            else 5 end index
      ,1.0 cnt
  from ${tmp_label_phone_year}

  union all
  select device
        ,case when cnt=1 then 6
              when cnt in (2,3) then 7
              when cnt in (4,5) then 8
              when cnt>5 then 9
              else 10 end index
        ,1.0 cnt
  from ${tmp_label_bssid_num}

  union all
  select device
        ,case when distance < 50 then 11
              when distance < 500 then 12
              when distance < 5000 then 13
              when distance >= 5000 then 14
              else 15 end index
        ,1.0 cnt
  from ${tmp_label_distance_avg}

  union all
  select device
        ,case when distance < 50 then 16
              when distance < 500 then 17
              when distance < 5000 then 18
              when distance >= 5000 then 19
              else 20 end index
        ,1.0 cnt
  from ${tmp_label_distance_night}

  union all
  select device
        ,case when home_work_dist=0 then 21
              when home_work_dist<=1000 then 22
              when home_work_dist<=10000 then 23
              when home_work_dist<=50000 then 24
              when home_work_dist>50000 then 25
              else 26 end index
        ,1.0 cnt
  from ${tmp_label_homeworkdist}

  union all
  select device,cast(26+cast(poi_type as double) as int) index,1.0 cnt
  from ${tmp_label_home_poiaround}

  union all
  select device,cast(46+cast(poi_type as double) as int) index,1.0 cnt
  from ${tmp_label_work_poiaround}
)a
group by device
"

## 删除中间临时表7天前数据
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
