#!/bin/sh

set -x -e

day=$1
day01=${day:0:6}01

p30day=`date -d "$day01 -1 months" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties


:'
input:
dm_mobdi_topic.ads_device_travel_di_old
dm_sdk_mapping.app_category_mapping_par
dm_mobdi_report.rp_device_profile_full_view

out:
dm_mobdi_report.travel_label_monthly
dw_mobdi_tmp.travel_cn_tmp
dw_mobdi_tmp.travel_area_tmp
dw_mobdi_tmp.travel_type_tmp
dw_mobdi_tmp.travel_traffic_tmp
dw_mobdi_tmp.travel_time_tmp
dw_mobdi_tmp.travel_channel_tmp
dw_mobdi_tmp.travel_info_tmp
'
#out
travel_cn_tmp=dw_mobdi_tmp.travel_cn_tmp_${day01}
travel_area_tmp=dw_mobdi_tmp.travel_area_tmp_${day01}
travel_type_tmp=dw_mobdi_tmp.travel_type_tmp_${day01}
travel_traffic_tmp=dw_mobdi_tmp.travel_traffic_tmp_${day01}
travel_time_tmp=dw_mobdi_tmp.travel_time_tmp_${day01}
travel_channel_tmp=dw_mobdi_tmp.travel_channel_tmp_${day01}
travel_info_tmp=dw_mobdi_tmp.travel_info_tmp


hive -e"
create table if not exists $travel_label_monthly(
device string,
travel_area string comment '出行大区',
country string comment '出行国家/地区',
province_flag int comment '大陆地区偏好',
province string comment '大陆地区省份偏好',
city string comment '大陆地区城市偏好',
vaca_flag string comment '出行时段',
travel_type string comment '出行类型',
traffic string comment '交通方式偏好',
travel_time string comment '出行时长',
travel_channel string comment '出行预定渠道'
) comment '旅游出行偏好'
partitioned by (day string)
stored as orc;

create table if not exists $travel_cn_tmp(
device string,
province_flag string,
province string,
city string
) 
stored as orc;

--出行时段
--出行大区
--出行国家/地区
create table  if not exists $travel_area_tmp(
device string,
vaca_flag string,
travel_area string,
country string,
cheap_flight_installed string,
flight_installed string,
ticket_installed string,
car string,
rentcar_installed string,
business_flag string,
busi_app_act string,
travel_app_act string,
poi_flag string,
ticket_active string,
rentcar_active string
) stored as orc;


create table  if not exists $travel_type_tmp (
device string,travel_type string
)
stored as orc;


create table if not exists $travel_traffic_tmp(
device string,
traffic string
)
stored as orc;


create table if not exists $travel_time_tmp(
device string,
travel_time string
)
stored as orc;


create table if not exists $travel_channel_tmp(
device string,
travel_channel string
)
stored as orc;


create table if not exists $travel_info_tmp(
device string,
travel_area string comment '出行大区',
country string comment '出行国家/地区',
province_flag int comment '大陆地区偏好',
province string comment '大陆地区省份偏好',
city string comment '大陆地区城市偏好',
vaca_flag string comment '出行时段',
travel_type string comment '出行类型',
traffic string comment '交通方式偏好',
travel_time string comment '出行时长',
travel_channel string comment '出行预定渠道'
) comment '旅游出行偏好'
partitioned by (day string)
stored as orc;
"


:<<!
   出行类型
   枚举值：商务差旅\自助游\跟团游
   商务差旅：商务人士标签或者时间段内使用商务类app
   自助游：去过景点且时间段内使用自由行类app或者有车的设备
   跟团游：去过景点且无上述四个标签行为的人群
!

hive -e"
insert overwrite table $travel_cn_tmp
select device,
max(province_flag) as province_flag,
concat_ws(',',collect_set(province)) as province,
concat_ws(',',collect_set(if(city = '',null,city))) as city
from $ads_device_travel_di_old
where day >= '$p30day' 
and day < '$day01'
and country = 'cn' 
and province not in ('cn_31','cn_32','cn_33')
group by device

"

hive -e"
insert overwrite table $travel_area_tmp
select device,
concat_ws(',',collect_set(
case when vaca_flag = 1 then '小长假'
     when vaca_flag = 2 then '大长假'
     when vaca_flag = 3 then '工作日'
else '双休日' 
end)) as vaca_flag,
concat_ws(',',collect_set(travel_area)) as travel_area,
concat_ws(',',collect_set(country)) as country,
max(cheap_flight_installed) as cheap_flight_installed,
max(flight_installed) as flight_installed,
max(ticket_installed) as ticket_installed,
max(car) as car,
max(rentcar_installed) as rentcar_installed,
max(business_flag) as business_flag,
max(busi_app_act) as busi_app_act,
max(travel_app_act) as travel_app_act,
max(poi_flag) as poi_flag,
max(ticket_active) as ticket_active,
max(rentcar_active) as rentcar_active
from $ads_device_travel_di_old
where day >= '$p30day' and day < '$day01' and length(travel_area) > 0
group by device;
"

hive -e"
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
ADD JAR hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function remove_null_str as 'com.youzu.mob.java.udf.RemoveNullStr';

insert overwrite table  $travel_type_tmp
select device,remove_null_str(travel_type) as travel_type
from
(
  select device,concat_ws(',',
  case when business_flag = 1 or busi_app_act = 1 then '商务差旅' else '' end,
  case when (travel_app_act = 1 or car = 1) and poi_flag = 1 then '自助游' else '' end,
  case when (business_flag+busi_app_act+travel_app_act+car) = 0 and poi_flag = 1 then '跟团游' else '' end
  ) as travel_type
  from $travel_area_tmp
)travel_area_tmp;

insert overwrite table  $travel_traffic_tmp
select device,remove_null_str(traffic) as traffic
from
(
  select device,concat_ws(',',
  case when cheap_flight_installed = 1 then '廉价航空' else '' end,
  case when flight_installed = 1 then '一般航空' else '' end,
  case when ticket_installed = 1 or ticket_active = 1 then '轨道交通' else '' end,
  case when car = 1 or rentcar_installed = 1 or rentcar_active = 1 then '自驾' else '' end,
  case when (cheap_flight_installed+flight_installed+ticket_installed+ticket_active+car+rentcar_installed+rentcar_active) = 0 then '巴士'
  else '' end) as traffic
  from $travel_area_tmp
)travel_area_tmp
"

:<<!
--大陆地区偏好（定义：是否出行周边省份）
--大陆地区省份偏好
--大陆地区城市偏好
!
hive -e"
insert overwrite table $travel_time_tmp
select device,
case when cnt >= 1 and cnt <= 3 then '1-3天'
     when cnt >= 4 and cnt <= 5 then '4-5天'
else '6天及以上'
end as travel_time 
from
(
  select device,count(1) as cnt
  from
    (
    select device,day 
    from $ads_device_travel_di_old
    where day >= '$p30day' and day < '$day01'
    group by device,day
    )travel_daily
  group by device
)travel_daily_cnt
"


hive -e"
insert overwrite table $travel_channel_tmp
select device,concat_ws(',',collect_set(appname)) as travel_channel
from
(
  select device,app 
  from
  (
    select travel_time_tmp.device,device_profile_full.applist 
    from
    (
     select device from $travel_time_tmp
    )travel_time_tmp
    inner join
    (
     select device,applist from $rp_device_profile_full_view
    )device_profile_full
    on travel_time_tmp.device = device_profile_full.device
  )applist
  lateral view explode(split(applist,',')) t as app
)t1
inner join
(
  select apppkg,min(appname) as appname
  from $app_category_mapping_par
  where version='1000' and cate_l1 = '旅游出行' and cate_l2 = '在线旅行'
  group by apppkg
)app_category_mapping_par
on t1.app = app_category_mapping_par.apppkg
group by device
"

hive -e"
insert overwrite table $travel_info_tmp partition(day='$day01')
select device,max(travel_area) as travel_area,max(country) as country,max(province_flag) as province_flag,max(province) as province,max(city) as city,max(vaca_flag) as vaca_flag,max(travel_type) as travel_type,max(traffic) as traffic,max(travel_time) as travel_time,max(travel_channel) as travel_channel
from
(
  select device,'' as travel_area,'' as country,province_flag,province,city,'' as vaca_flag,'' as travel_type,'' as traffic,'' as travel_time,'' as travel_channel 
  from $travel_cn_tmp
  union all
  select device,travel_area,country,0 as province_flag,'' as province,'' as city,vaca_flag,'' as travel_type,'' as traffic,'' as travel_time,'' as travel_channel 
  from $travel_area_tmp
  union all
  select device,'' as travel_area,'' as country,0 as province_flag,'' as province,'' as city,'' as vaca_flag,travel_type,'' as traffic,'' as travel_time,'' as travel_channel 
  from $travel_type_tmp
  union all
  select device,'' as travel_area,'' as country,0 as province_flag,'' as province,'' as city,'' as vaca_flag,'' as travel_type,traffic,'' as travel_time,'' as travel_channel 
  from $travel_traffic_tmp
  union all
  select device,'' as travel_area,'' as country,0 as province_flag,'' as province,'' as city,'' as vaca_flag,'' as travel_type,'' traffic,travel_time,'' as travel_channel 
  from $travel_time_tmp
  union all
  select device,'' as travel_area,'' as country,0 as province_flag,'' as province,'' as city,'' as vaca_flag,'' as travel_type,'' traffic,'' as travel_time,travel_channel 
  from $travel_channel_tmp
)all_info
group by device
"


hive -e"
insert overwrite table $travel_label_monthly partition(day='$day01')
select device,
travel_area,
country,
province_flag,
province,
city,
vaca_flag,
travel_type,
traffic,
travel_time,
travel_channel 
from $travel_info_tmp where day='$day01'
"


#删除临时表
hive -e"
drop table if exists $travel_cn_tmp;
drop table if exists $travel_area_tmp;
drop table if exists $travel_type_tmp;
drop table if exists $travel_traffic_tmp;
drop table if exists $travel_time_tmp;
drop table if exists $travel_channel_tmp;
"

