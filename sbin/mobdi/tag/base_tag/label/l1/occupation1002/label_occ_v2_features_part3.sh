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
#input
device_applist_new=${dim_device_applist_new_di}
gdpoi_explode_big="dm_sdk_mapping.mapping_gdpoi_explode_big"
mapping_phonenum_year="dm_sdk_mapping.mapping_phonenum_year"


tmp_occ1002_work_poi_2=${tmpdb}.tmp_occ1002_work_poi_2_${day}
tmp_label_occ1002_predict_workpoi_mid=${tmpdb}.tmp_label_occ1002_predict_workpoi_mid_${day}

tmp_tables=($tmp_occ1002_work_poi_2 $tmp_label_occ1002_predict_workpoi_mid)

output_table=${tmpdb}.tmp_occ1002_predict_part3
## part3添加tmp_work_poi_2,不能完全复用,可以和年龄part3合并
:<<!
HADOOP_USER_NAME=dba hive -e"
drop table if exists ${tmpdb}.tmp_occ1002_label_phone_year;
create table ${tmpdb}.tmp_occ1002_label_phone_year stored as orc as
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
        join dim_mobdi_mapping.android_id_mapping_full_view b
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

drop table if exists ${tmpdb}.tmp_occ1002_label_bssid_num;
create table ${tmpdb}.tmp_occ1002_label_bssid_num stored as orc as
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

drop table if exists ${tmpdb}.tmp_occ1002_label_distance_avg;
create table ${tmpdb}.tmp_occ1002_label_distance_avg stored as orc as
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

drop table if exists ${tmpdb}.tmp_occ1002_label_distance_night;
create  table ${tmpdb}.tmp_occ1002_label_distance_night stored as orc as
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

drop table if exists ${tmpdb}.tmp_occ1002_label_homeworkdist;
create  table ${tmpdb}.tmp_occ1002_label_homeworkdist stored as orc as
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
    \"lbsSql\": \"  select device,lat_home lat,lon_home lon from ${tmpdb}.tmp_occ1002_label_homeworkdist  where lat_home is not null \",
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
    \"targetTable\": \"${tmpdb}.tmp_occ1002_home_poi\"
}"

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 12G \
--executor-memory 35G \
/home/dba/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_work lat,lon_work lon from ${tmpdb}.tmp_occ1002_label_homeworkdist  where lat_work is not null \",
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
    \"targetTable\": \"${tmpdb}.tmp_occ1002_work_poi\"
}"

HADOOP_USER_NAME=dba hive -e"
drop table if exists ${tmpdb}.tmp_occ1002_label_home_poiaround;
create table ${tmpdb}.tmp_occ1002_label_home_poiaround stored as orc as
select device,poi_type
from ${tmpdb}.tmp_occ1002_home_poi
group by device,poi_type
;
drop table if exists ${tmpdb}.tmp_occ1002_label_work_poiaround;
create table ${tmpdb}.tmp_occ1002_label_work_poiaround stored as orc as
select device,poi_type
from ${tmpdb}.tmp_occ1002_work_poi
group by device,poi_type
;
"
!
###############添加tmp_work_poi_2####################
HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.poi.PoiExport \
--master yarn \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--driver-memory 12G \
--executor-memory 35G \
/home/dba/lib/mobdi-poi-tool-v0.1.0.jar  \
"{
    \"dataType\": \"1\",
    \"lbsSql\": \"  select device,lat_work lat,lon_work lon from ${appdb}.label_homeworkdist_${day} where lat_work is not null \",
    \"poiTable\": \"$gdpoi_explode_big\",
    \"poiFields\": \"poi_id,name,lat,lon,type\",
    \"poiCalFields\": {
        \"distance\": {
            \"distanceRange\": \"200\"
        }
    },
    \"poiCondition\": {
        \"type\": \" '1401','1402','1403','1404','1405','1406','1407','1408','1409','1410','1411','1412','1413','1414','1415','1700','1701','1702','1703','1704' \"
    },
    \"targetTable\": \"${tmp_occ1002_work_poi_2} \"
}"

HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${tmp_label_occ1002_predict_workpoi_mid};
create table ${tmp_label_occ1002_predict_workpoi_mid} as
with seed as
(
  select *
  from $device_applist_new
  where day = '$day'
)
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
      from seed a 
      join ${tmp_occ1002_work_poi_2} b
      on a.device=b.device
      group by a.device,b.poi_type
      )x
;
"

HADOOP_USER_NAME=dba hive -v -e "
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
  from ${appdb}.label_phone_year_${day}

  union all
  select device
        ,case when cnt=1 then 6
              when cnt in (2,3) then 7
              when cnt in (4,5) then 8
              when cnt>5 then 9
              else 10 end index
        ,1.0 cnt
  from ${appdb}.label_bssid_num_${day}

  union all
  select device
        ,case when distance < 50 then 11
              when distance < 500 then 12
              when distance < 5000 then 13
              when distance >= 5000 then 14
              else 15 end index
        ,1.0 cnt
  from ${appdb}.label_distance_avg_${day}

  union all
  select device
        ,case when distance < 50 then 16
              when distance < 500 then 17
              when distance < 5000 then 18
              when distance >= 5000 then 19
              else 20 end index
        ,1.0 cnt
  from ${appdb}.label_distance_night_${day}

  union all
  select device
        ,case when home_work_dist=0 then 21
              when home_work_dist<=1000 then 22
              when home_work_dist<=10000 then 23
              when home_work_dist<=50000 then 24
              when home_work_dist>50000 then 25
              else 26 end index
        ,1.0 cnt
  from ${appdb}.label_homeworkdist_${day}

  union all
  select device,cast(26+cast(poi_type as double) as int) index,1.0 cnt
  from ${appdb}.label_home_poiaround_${day}

  union all
  select device,cast(46+cast(poi_type as double) as int) index,1.0 cnt
  from ${appdb}.label_work_poiaround_${day}

  union all
  select device,index,cnt 
  from ${tmp_label_occ1002_predict_workpoi_mid}
)a
group by device
"

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
