#!/bin/sh

set -x -e

: '
@owner:hugl
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

##tmpdb="mobdi_test"
##appdb="mobdi_test"
output_table="${tmpdb}.tmp_income1001_part3"
#input
device_applist_new=${dim_device_applist_new_di}
gdpoi_explode_big="dm_sdk_mapping.mapping_gdpoi_explode_big"
mapping_phonenum_year="dm_sdk_mapping.mapping_phonenum_year"

## part3完全复用年龄part3

HADOOP_USER_NAME=dba hive -e"
drop table if exists ${appdb}.label_income1001_v2_phone_year;
create table ${appdb}.label_income1001_v2_phone_year stored as orc as
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
        join dm_mobdi_mapping.android_id_mapping_full_view b
        on a.device=b.device
      )c lateral view explode_tags(phone_list) n as phone,pn_tm
    )d       where length(phone) = 11
  )e where rn=1
)x
left join
(select * from $mapping_phonenum_year where version='1000')y
on substr(x.phone,1,3)=y.phone_pre3
"


HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
create temporary function get_distance as 'com.youzu.mob.java.udf.GetDistance';

drop table if exists ${appdb}.label_income1001_v2_bssid_num;
create table ${appdb}.label_income1001_v2_bssid_num stored as orc as
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
dw_mobdi_md.tmp_anticheat_device_bssid_cnt_30days
where day=GET_LAST_PARTITION('dw_mobdi_md', 'tmp_anticheat_device_bssid_cnt_30days', 'day')
)b
on a.device=b.device
)c where rn=1;

drop table if exists ${appdb}.label_income1001_v2_distance_avg;
create table ${appdb}.label_income1001_v2_distance_avg stored as orc as
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
  from dw_mobdi_md.tmp_anticheat_device_avgdistance_pre
  where day=GET_LAST_PARTITION('dw_mobdi_md', 'tmp_anticheat_device_avgdistance_pre', 'day') and timewindow='30'
)b
on a.device=b.device
)c where rn=1
;

drop table if exists ${appdb}.label_income1001_v2_distance_night;
create  table ${appdb}.label_income1001_v2_distance_night stored as orc as
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
  from dw_mobdi_md.tmp_anticheat_device_nightdistance_pre
  where day=GET_LAST_PARTITION('dw_mobdi_md', 'tmp_anticheat_device_nightdistance_pre', 'day') and timewindow='30'
)b
on a.device=b.device
)c where rn=1
;

drop table if exists ${appdb}.label_income1001_v2_homeworkdist;
create  table ${appdb}.label_income1001_v2_homeworkdist stored as orc as
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


HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
/home/dba/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_home lat,lon_home lon from ${appdb}.label_income1001_v2_homeworkdist  where lat_home is not null \",
    \"poiTable\": \"$gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \"'01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20' \"
    },
    \"targetTable\": \"${tmpdb}.tmp_income1001_v2_home_poi\"
}"


HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--queue yarn_analyst.analyst2 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 12G \
--executor-memory 35G \
/home/dba/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_work lat,lon_work lon from ${appdb}.label_income1001_v2_homeworkdist  where lat_work is not null \",
    \"poiTable\": \"$gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \" '01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20' \"
    },
    \"targetTable\": \"${tmpdb}.tmp_income1001_v2_work_poi\"
}"


HADOOP_USER_NAME=dba hive -e"
drop table if exists ${appdb}.label_income1001_v2_home_poiaround;
create table ${appdb}.label_income1001_v2_home_poiaround stored as orc as
select device,poi_type
from ${tmpdb}.tmp_income1001_v2_home_poi
group by device,poi_type
;
drop table if exists ${appdb}.label_income1001_v2_work_poiaround;
create  table ${appdb}.label_income1001_v2_work_poiaround stored as orc as
select device,poi_type
from ${tmpdb}.tmp_income1001_v2_work_poi
group by device,poi_type
;
"



HADOOP_USER_NAME=dba hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

drop table if exists ${output_table};
create table if not exists ${output_table} stored as orc as
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
  from ${appdb}.label_income1001_v2_phone_year

  union all
  select device
        ,case when cnt=1 then 6
              when cnt in (2,3) then 7
              when cnt in (4,5) then 8
              when cnt>5 then 9
              else 10 end index
        ,1.0 cnt
  from ${appdb}.label_income1001_v2_bssid_num

  union all
  select device
        ,case when distance < 50 then 11
              when distance < 500 then 12
              when distance < 5000 then 13
              when distance >= 5000 then 14
              else 15 end index
        ,1.0 cnt
  from ${appdb}.label_income1001_v2_distance_avg

  union all
  select device
        ,case when distance < 50 then 16
              when distance < 500 then 17
              when distance < 5000 then 18
              when distance >= 5000 then 19
              else 20 end index
        ,1.0 cnt
  from ${appdb}.label_income1001_v2_distance_night

  union all
  select device
        ,case when home_work_dist=0 then 21
              when home_work_dist<=1000 then 22
              when home_work_dist<=10000 then 23
              when home_work_dist<=50000 then 24
              when home_work_dist>50000 then 25
              else 26 end index
        ,1.0 cnt
  from ${appdb}.label_income1001_v2_homeworkdist

  union all
  select device,cast(26+cast(poi_type as double) as int) index,1.0 cnt
  from ${appdb}.label_income1001_v2_home_poiaround

  union all
  select device,cast(46+cast(poi_type as double) as int) index,1.0 cnt
  from ${appdb}.label_income1001_v2_work_poiaround
)a
group by device
"

