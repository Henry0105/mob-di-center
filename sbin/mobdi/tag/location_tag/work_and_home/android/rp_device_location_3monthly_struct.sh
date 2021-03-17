#!/bin/sh

: '
@owner:zhangxy
@describe:合并工作地居住地各列，导入profile_full表，跟在rp_mobdi_app.rp_device_location_3monthly后面跑
@projectName:mobdi
@BusinessName:
@SourceTable:rp_mobdi_app.rp_device_location_3monthly
@TargetTable:rp_mobdi_app.rp_device_location_3monthly_struct
@TableRelation:rp_mobdi_app.rp_device_location_3monthly -> rp_mobdi_app.rp_device_location_3monthly_struct
'


set -x -e
export LANG=en_US.UTF-8


source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

day_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function get_last_partition as 'com.youzu.mob.java.udf.LatestPartition';
select get_last_partition('dm_mobdi_report', 'rp_device_location_3monthly', 'day');
drop temporary function get_last_partition;
"
day=(`hive -e "set hive.cli.print.header=false;$day_sql"`)


: '
@part_1:
实现功能：工作地居住地各列合并

实现逻辑：lat_home,lon_home,province_home,city_home,area_home,street(置为空),cnt_home,confidence_home合并为residence列,
lat_work,lon_work,province_work,city_work,area_work,street(置为空),cnt_work,confidence_work合并为workplace,
其中将null值和unknown值均置为空，
若合并之后residence或workplace为“lat:,lon:,province:,city:,area:,street:,cnt:,confidence:”，即没有工作地或居住地，则整个字段置为空；
'

hive -e"
create table if not exists dm_mobdi_report.rp_device_location_3monthly_struct(
device string COMMENT '设备号',
workplace string COMMENT '工作地,confidence表示置信度',
residence string COMMENT '居住地,confidence表示置信度')
COMMENT '工作地居住地各列合并表'
partitioned by (
day string)
stored as orc;
"


hive -e"
insert overwrite table $rp_device_location_3monthly_struct partition(day = '$day')
select device,
case when work = 'lat:,lon:,province:,city:,area:,street:,cnt:,confidence:' then '' else work end as workplace,
case when home = 'lat:,lon:,province:,city:,area:,street:,cnt:,confidence:' then '' else home end as residence
from 
(
  select device,   
  concat_ws(',', concat('lat:', lat_work), concat('lon:', lon_work), concat('province:', province_work), concat('city:', city_work), concat('area:', area_work), concat('street:', ''), concat('cnt:', cnt_work), concat('confidence:', confidence_work)) as work,
  concat_ws(',', concat('lat:', lat_home), concat('lon:', lon_home), concat('province:', province_home), concat('city:', city_home), concat('area:', area_home), concat('street:', ''), concat('cnt:', cnt_home), concat('confidence:', confidence_home)) as home
  from 
  (
    select device, 
    case when lat_home is null or lat_home = 'unknown' then '' else lat_home end as lat_home,
    case when lon_home is null or lon_home = 'unknown' then '' else lon_home end as lon_home,
    case when province_home is null or province_home = 'unknown' then '' else province_home end as province_home,
    case when city_home is null or city_home = 'unknown' then '' else city_home end as city_home,
    case when area_home is null or area_home = 'unknown' then '' else area_home end as area_home,
    case when cnt_home is null or cnt_home = 'unknown' then '' else cnt_home end as cnt_home,
    case when confidence_home is null or confidence_home = 'unknown' then '' else confidence_home end as confidence_home,
    case when lat_work is null or lat_work = 'unknown' then '' else lat_work end as lat_work,
    case when lon_work is null or lon_work = 'unknown' then '' else lon_work end as lon_work,
    case when province_work is null or province_work = 'unknown' then '' else province_work end as province_work,
    case when city_work is null or city_work = 'unknown' then '' else city_work end as city_work,
    case when area_work is null or area_work = 'unknown' then '' else area_work end as area_work,
    case when cnt_work is null or cnt_work = 'unknown' then '' else cnt_work end as cnt_work,
    case when confidence_work is null or confidence_work = 'unknown' then '' else confidence_work end as confidence_work
    from $rp_device_location_3monthly
    where day = '$day'
  ) as a
) as b;
"