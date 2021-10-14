#!/bin/bash

set -e -x

day=$1
days=${day:0:6}01


p1month=`date -d "$days -1 month" +%Y%m`
p4month=`date -d "$days -4 month" +%Y%m`
p30day=`date -d "$days -1 month" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dm_mobdi_master.dwd_device_location_info_di_v2
##mapping
#dim_mapping_base_station_location=dim_sdk_mapping.dim_mapping_base_station_location
#dm_sdk_mapping.mapping_base_station_location
#dm_sdk_mapping.vacation_flag_par
#dim_mapping_ip_attribute_code=dim_sdk_mapping.dim_mapping_ip_attribute_code
#dm_sdk_mapping.mapping_ip_attribute_code

base_station_location_db=${dim_mapping_base_station_location%.*}
base_station_location_tb=${dim_mapping_base_station_location#*.}
ip_attribute_code_db=${dim_mapping_ip_attribute_code%.*}
ip_attribute_code_tb=${dim_mapping_ip_attribute_code#*.}
vacation_flag_db=${dim_vacation_flag_par%.*}
vacation_flag_tb=${dim_vacation_flag_par#*.}
sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$vacation_flag_db', '$vacation_flag_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
vacation_flag_lastday=(`hive  -e "$sql"`)

#out
tmp_device_location_summary_monthly=dm_mobdi_tmp.tmp_device_location_summary_monthly


hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set mapreduce.task.timeout=1200000;

--- type 只保存 gps tlocation
insert overwrite TABLE $tmp_device_location_summary_monthly partition(month='${p1month}',type=1)
SELECT
    device,
    case when lon  between -180 and 180 then round(lon,5) else '' end as lon,
    case when lat  between -90 and 90 then round(lat,5) else '' end as lat,
    time,
    day,
    '' as station,
    '' as wifi_type
FROM
    $dwd_device_location_info_di_v2
where day >='${p30day}' and day<'${days}'
and data_source not in ('pv','run')
and lon not in('','0.0','0.0065') and lat not in ('','0.0','0.006')
and plat=1 and type in('gps','tlocation')
"

# 只保留最近三个分区
hive -e"
alter table $tmp_device_location_summary_monthly drop partition(month='${p4month}',type=1);
"

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

insert overwrite TABLE $tmp_device_location_summary_monthly partition(month='${p1month}',type=2)
select device,nvl(b.lon,a.lon) as lon,nvl(b.lat,a.lat) as lat,time,day,b.network as station,'' as wifi_type
from
(
SELECT
    device,
    case when lon  between -180 and 180 then round(lon,5) else '' end as lon,
    case when lat  between -90 and 90 then round(lat,5) else '' end as lat,
    time,
    a.day,
    split(split(orig_note1,'=')[1],',')[0] as lac ,
    split(split(orig_note1,'=')[1],',')[1] cell,
    split(split(orig_note2,'=')[1],',')[1] as mnc
FROM
    $dwd_device_location_info_di_v2 a
where a.day >='${p30day}' and a.day<'${days}'
and data_source ='base'
and plat=1 and type ='base'
and not EXISTS(
    select day from $dim_vacation_flag_par tt where tt.version='$vacation_flag_lastday' and tt.flag != 3 and a.day=tt.day
)
)a
left join
(select lac,cell,mnc,lon,lat,network from $dim_mapping_base_station_location
where day=GET_LAST_PARTITION('$base_station_location_db','$base_station_location_tb'))b
on a.lac = b.lac and a.cell = b.cell and a.mnc = b.mnc
where coalesce(b.lon,a.lon,'') not in('','0.0','0.0065') and coalesce(b.lat,a.lat,'') not in ('','0.0','0.006')
;
"
# 只保留最近三个分区
hive -e"
alter table $tmp_device_location_summary_monthly drop partition(month='${p4month}',type=2);
"

hive -e"

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

insert overwrite TABLE $tmp_device_location_summary_monthly partition(month='${p1month}',type=3)
SELECT
    device,
    case when lon  between -180 and 180 then round(lon,5) else '' end as lon,
    case when lat  between -90 and 90 then round(lat,5) else '' end as lat,
    time,
    a.day,
    '' as station,
    split(orig_note3,'=')[1] as wifi_type
FROM
    $dwd_device_location_info_di_v2 a
where a.day >='${p30day}' and a.day<'${days}'
and plat=1 and data_source ='wifi' and type='wifi'
and lon not in('','0.0','0.0065') and lat not in('','0.0','0.006')
and not EXISTS(
    select day from $dim_vacation_flag_par tt where tt.version='$vacation_flag_lastday' and tt.flag != 3 and a.day=tt.day
)
;
"

# 只保留最近三个分区
hive -e"
alter table $tmp_device_location_summary_monthly drop partition(month='${p4month}',type=3);
"


hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;

create temporary function get_min_ip as 'com.youzu.mob.java.udf.GetIpAttribute';
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';

insert overwrite TABLE $tmp_device_location_summary_monthly partition(month='${p1month}',type=4)
select device,nvl(b.lon,a.lon) as lon,nvl(b.lat,a.lat) as lat,time,day,network as station,area as wifi_type
from
(
SELECT
    device,
    time,
    a.day,
    network,
    case when lon  between -180 and 180 then round(lon,5) else '' end as lon,
    case when lat  between -90 and 90 then round(lat,5) else '' end as lat,
    get_min_ip(split(orig_note1,'=')[1]) as minip
FROM
    $dwd_device_location_info_di_v2 a
where a.day >='${p30day}' and a.day<'${days}'
and plat=1 and type ='ip'
and not EXISTS(
    select day from $dim_vacation_flag_par tt where tt.version='$vacation_flag_lastday' and tt.flag != 3 and a.day=tt.day
)
)a
left join
(select minip,bd_lon as lon,bd_lat as lat,area_code as area from $dim_mapping_ip_attribute_code
 where day=GET_LAST_PARTITION('$ip_attribute_code_db','$ip_attribute_code_tb'))b
on (case when a.minip ='-1' then concat('',rand()) else a.minip end) = b.minip
where coalesce(b.lon,a.lon,'') not in('','0.0','0.0065') and coalesce(b.lat,a.lat,'') not in ('','0.0','0.006')
;
"
# 只保留最近三个分区
hive -e"
alter table $tmp_device_location_summary_monthly drop partition(month='${p4month}',type=4);
"
