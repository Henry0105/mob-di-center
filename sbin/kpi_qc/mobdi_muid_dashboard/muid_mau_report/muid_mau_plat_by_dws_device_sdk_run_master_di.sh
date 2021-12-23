#!/bin/bash

set -e -x

: '
@owner:baron
@describe:muid 月活指标
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
create table if not exists mobdi_muid_dashboard.muid_mau_plat_by_dws_device_sdk_run_master_di(
muid_mau bigint 'muid月活',
plat string comment '1-android,2-ios'
)comment 'muid月活'
partitioned by (day string comment'日期')
stored as orc;
!

#muid月活
HADOOP_USER_NAME=dba hive -v -e "
set hive.exec.parallel=true;
insert overwrite table mobdi_muid_dashboard.muid_mau_plat_by_dws_device_sdk_run_master_di partition(day='$day')
select count(*) as mau,plat
from
(
select device, '2' as plat from
mobdi_muid_dashboard.muid_mau_ios_by_dws_device_sdk_run_master_di_pre
union all
select device, '1' as plat from 
mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre
) as a
group by plat;
"
