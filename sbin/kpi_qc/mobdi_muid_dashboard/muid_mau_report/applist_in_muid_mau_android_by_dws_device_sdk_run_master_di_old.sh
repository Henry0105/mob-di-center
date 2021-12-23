#!/bin/bash

set -e -x

: '
@owner:baron
@describe:muid 月活muid中有applist的量
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
create table if not exists mobdi_muid_dashboard.applist_in_muid_mau_android_by_dws_device_sdk_run_master_di(
applist_mau_his bigint comment'月活muid中有applist的量(历史采集到过applist就算)',
applist_mau_now bigint comment'月活muid中有applist的量(当月采集到的才算)'
)comment '月活muid中有applist的量'
partitioned by (day string comment'日期')
stored as orc;
!
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_mobdashboard.mobdashboard;
insert overwrite table mobdi_muid_dashboard.applist_in_muid_mau_android_by_dws_device_sdk_run_master_di partition(day='$day')
select
count(*) as applist_mau_his,sum(if(processtime >= '$firstDayOfMonth' and processtime <= '$day',1,0)) as applist_mau_now
from
(
select a.device, processtime
from
mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a
inner join
(
  select device, processtime
  from dm_mobdi_report.device_profile_label_full_par
  where version = '$day' and applist not in ('', 'unknown')
  group by device, processtime
) as b
on a.device = b.device
)c;
"
