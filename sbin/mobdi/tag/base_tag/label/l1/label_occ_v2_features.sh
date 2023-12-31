#!/bin/bash
set -x -e
: '
@owner:yeyy
@describe: device的职业模型feature
@projectName:MOBDI
'
# 无输出表

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb="${dm_mobdi_tmp}"
mddb="$dw_mobdi_md"
appdb="$rp_mobdi_report"
#input
device_applist_new=${dim_device_applist_new_di}

#mapping
#mapping_age_cate_index1="dim_sdk_mapping.mapping_age_cate_index1"
#mapping_app_cate_index2="dim_sdk_mapping.mapping_age_cate_index2"
#mapping_occ_app_index="dim_sdk_mapping.mapping_occ_app_index"
#dim_pid_release_year_china="dim_sdk_mapping.mapping_phonenum_year"
#dim_mapping_gdpoi_explode_big="dim_sdk_mapping.mapping_gdpoi_explode_big"
#dim_gdpoi_explode_mid="dim_sdk_mapping.dim_gdpoi_explode_mid"
#output

label_occ_app2vec=${appdb}.label_occ_app2vec
label_occ_score_applist=${appdb}.label_occ_score_applist
label_occ_phone_year=${appdb}.label_occ_phone_year
label_occ_bssid_num=${appdb}.label_occ_bssid_num
label_occ_distance_avg=${appdb}.label_occ_distance_avg
label_occ_distance_night=${appdb}.label_occ_distance_night
label_occ_homeworkdist=${appdb}.label_occ_homeworkdist
tmp_score_work_poi=${appdb}.tmp_score_work_poi

tmp_occ_home_poi=${tmpdb}.tmp_occ_home_poi
tmp_occ_work_poi=${tmpdb}.tmp_occ_work_poi
tmp_work_poi=${tmpdb}.tmp_work_poi
tmp_home_poi=${tmpdb}.tmp_home_poi

apppkg_app2vec_db=${apppkg_app2vec_par_wi%.*}
apppkg_app2vec_tb=${apppkg_app2vec_par_wi#*.}
#logic
tmp_anticheat_device_nightdistance_pre=$mddb.tmp_anticheat_device_nightdistance_pre
tmp_anticheat_device_bssid_cnt_30days=$mddb.tmp_anticheat_device_bssid_cnt_30days
tmp_anticheat_device_avgdistance_pre=$mddb.tmp_anticheat_device_avgdistance_pre
nightdistance_db=${tmp_anticheat_device_nightdistance_pre%.*}
nightdistance_tb=${tmp_anticheat_device_nightdistance_pre#*.}
avgdistance_db=${tmp_anticheat_device_avgdistance_pre%.*}
avgdistance_tb=${tmp_anticheat_device_avgdistance_pre#*.}
bssid_cnt_db=${tmp_anticheat_device_bssid_cnt_30days%.*}
bssid_cnt_tb=${tmp_anticheat_device_bssid_cnt_30days#*.}


:<<!
hive -v -e "
--catel1,55-73
--放在label_apppkg_feature_category_index.sh里面
--9 cate l2 --74-287
"
!

{
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;

drop table if exists $label_occ_app2vec;
create table $label_occ_app2vec stored as orc as
with seed as
(
  select *
  from $device_applist_new
  where day = '$day'
)
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
  (select * from $apppkg_app2vec_par_wi where day=GET_LAST_PARTITION('$apppkg_app2vec_db', '$apppkg_app2vec_tb', 'day')) y
on x.pkg = y.apppkg
group by device;

"
} &

{
hive -v -e "
drop table if exists $label_occ_score_applist;
create  table $label_occ_score_applist stored as orc as
with seed as
(
  select *
  from $device_applist_new
  where day = '$day'
)
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
      select apppkg, index_after_chi index from $mapping_occ_app_index where version='1000'
    ) b
    on a.pkg=b.apppkg
  )c group by device
)y
on x.device=y.device
"
} &

{
hive -v -e "
drop table if exists $label_occ_phone_year;
create table $label_occ_phone_year stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select x.device,y.phone_pre3,y.year from
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
        join $id_mapping_android_df_view b
        on a.device=b.device
      )c lateral view explode_tags(phone_list) n as phone,pn_tm
    )d       where length(phone) = 11
  )e where rn=1
)x
left join
(select * from $dim_pid_release_year_china where version='1000')y
on substr(x.phone,1,3)=y.phone_pre3
"
} &

{
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
create temporary function get_distance as 'com.youzu.mob.java.udf.GetDistance';

drop table if exists $label_occ_bssid_num;
create table $label_occ_bssid_num stored as orc as
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

drop table if exists $label_occ_distance_avg;
create table $label_occ_distance_avg stored as orc as
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
  where day=GET_LAST_PARTITION('$avgdistance_db', '$avgdistance_tb', 'day') and timewindow='30'
)b
on a.device=b.device
)c where rn=1
;

drop table if exists $label_occ_distance_night;
create  table $label_occ_distance_night stored as orc as
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
  where day=GET_LAST_PARTITION('$nightdistance_db', '$nightdistance_tb', 'day') and timewindow='30'
)b
on a.device=b.device
)c where rn=1
;

drop table if exists $label_occ_homeworkdist;
create  table $label_occ_homeworkdist stored as orc as
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
  join $rp_device_profile_full_view b
  on a.device=b.device
)t;

"


spark2-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_home lat,lon_home lon from $label_homeworkdist where lat_home is not null \",
    \"poiTable\": \"$dim_mapping_gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \"'01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20' \"
    },
    \"targetTable\": \"$tmp_occ_home_poi \"
}"

spark2-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 5 \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_work lat,lon_work lon from $label_homeworkdist where lat_work is not null \",
    \"poiTable\": \"$dim_mapping_gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \" '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20' \"
    },
    \"targetTable\": \"$tmp_occ_work_poi \"
}"


/opt/mobdata/sbin/spark-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 4 \
/home/dba/mobdi_center/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_work lat,lon_work lon from $label_homeworkdist where lat_work is not null \",
    \"poiTable\": \" $dim_gdpoi_explode_mid \",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \" '1401','1402','1403','1404','1405','1406','1407','1408','1409','1410','1411','1412','1413','1414','1415','1700','1701','1702','1703','1704' \"
    },
    \"targetTable\": \"$tmp_occ_score_work_poi \"
}"

hive -v -e"
create table $label_occ_score_workpoi_mid as
select device
      ,case when poi_type='1401' then 67
            when poi_type='1402' then 68
            when poi_type='1403' then 69
            when poi_type='1404' then 70
            when poi_type='1405' then 71
            when poi_type='1406' then 72
            when poi_type='1407' then 73
            when poi_type='1408' then 74
            when poi_type='1409' then 75
            when poi_type='1410' then 76
            when poi_type='1411' then 77
            when poi_type='1412' then 78
            when poi_type='1413' then 79
            when poi_type='1414' then 80
            when poi_type='1415' then 81
            when poi_type='1700' then 82
            when poi_type='1701' then 83
            when poi_type='1702' then 84
            when poi_type='1703' then 85
            when poi_type='1704' then 86
            else 87 end index,1.0 cnt
from (select a.device,b.poi_type
      from $label_homeworkdist a
      join $tmp_score_work_poi b
      on a.device=b.device
      group by a.device,b.poi_type
      )x
;
"


hive -v -e "
drop table if exists $label_home_poiaround;
create table $label_home_poiaround stored as orc as
select device,poi_type
from $tmp_home_poi
group by device,poi_type
;
drop table if exists $label_work_poiaround;
create  table $label_work_poiaround stored as orc as
select device,poi_type
from $tmp_work_poi
group by device,poi_type
;
"

} &




wait
