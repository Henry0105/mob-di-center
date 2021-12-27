#!/bin/bash

set -e -x

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1
firstDayOfMonth=`date -d ${day} +%Y%m01`

#每月1号运行一次，如20211101运行，调度系统传来的day=20211031
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
drop table if exists mobdi_muid_dashboard.location_in_muid_mau;
create table mobdi_muid_dashboard.location_in_muid_mau stored as orc as 
select if(plat=1,muid,device) as device,'base' as location_type,plat
from dm_mobdi_master.dwd_base_station_info_sec_di
where day >= '$firstDayOfMonth' and day <= '$day' and plat in ('1','2') and (((lac is not null and cell is not null and lac <> '' and cell <> '') and ((bid is null and sid is null and nid is null)) ) 
or ((bid is not null and sid is not null and nid is not null) and ((lac is null and cell is null) or (lac = '' and cell = '')) ))
group by if(plat=1,muid,device),plat
union all
select if(plat=1,muid,deviceid) as device,'gps' as location_type,plat
from dm_mobdi_master.dwd_location_info_sec_di
where day >= '$firstDayOfMonth' and day <= '$day' and plat in ('1','2') and latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 and abs(latitude)<=90 and abs(longitude)<=180
group by if(plat=1,muid,deviceid),plat
union all
select if(plat=1,muid,deviceid) as device,'gps' as location_type,plat
from dm_mobdi_master.dwd_auto_location_info_sec_di
where day >= '$firstDayOfMonth' and day <= '$day' and plat in ('1','2') and latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 and abs(latitude)<=90 and abs(longitude)<=180
group by if(plat=1,muid,deviceid),plat
union all 
select if(plat=1,muid,deviceid) as device,'gps' as location_type,plat
from dm_mobdi_master.dwd_t_location_sec_di
where day >= '$firstDayOfMonth' and day <= '$day' and plat in ('1','2') and latitude is not null and longitude is not null and (latitude-round(latitude,1))*10<>0.0 and (longitude-round(longitude,1))*10<>0.0 and abs(latitude)<=90 and abs(longitude)<=180
group by if(plat=1,muid,deviceid),plat
union all
select if(plat=1,muid,device) as device,'wifi' as location_type,plat
from dm_mobdi_master.dwd_log_wifi_info_sec_di
where day >= '$firstDayOfMonth' and day <= '$day' and plat in ('1','2') 
and trim(bssid) not in ( '00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff', '', 'null', 'NULL') and bssid is not null and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
group by if(plat=1,muid,device),plat
"
