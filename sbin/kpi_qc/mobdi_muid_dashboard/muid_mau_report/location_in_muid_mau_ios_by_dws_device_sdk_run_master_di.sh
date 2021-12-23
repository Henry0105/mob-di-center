#!/bin/bash

set -e -x

: '
@owner:baron
@describe:muid 月活muid中有ieid的量
@projectName:mobdi_muid_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1
firstDayOfMonth=`date -d ${day} +%Y%m01`
:<<!
create table if not exists mobdi_muid_dashboard.location_in_muid_mau_ios_by_dws_device_sdk_run_master_di(
location_mau bigint comment '月活muid中有location的量',
location_gps_mau bigint comment '月活muid中有gps的量',
location_wifi_mau bigint comment '月活muid中有wifi的量',
location_base_mau bigint comment '月活muid中有base的量'
)comment 'ios版本 月活muid中有地理位置的量'
partitioned by (day string comment'日期')
stored as orc;
!
HADOOP_USER_NAME=dba hive -v -e "
insert overwrite table mobdi_muid_dashboard.location_in_muid_mau_ios_by_dws_device_sdk_run_master_di partition(day='$day')
select count(distinct(device)) as location_mau,sum(if(location_type ='gps',1,0)) as location_gps_mau,
sum(if(location_type ='wifi',1,0)) as location_wifi_mau,sum(if(location_type = 'base',1,0)) as location_base_mau
from
(select a.device,b.location_type as location_type
from
mobdi_muid_dashboard.muid_mau_ios_by_dws_device_sdk_run_master_di_pre a
inner join
(
  select device, location_type
  from mobdi_muid_dashboard.location_in_muid_mau
  where  plat = 2 
) as b 
on a.device = b.device
)c;
"

