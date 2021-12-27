#!/bin/bash

set -e -x

: '
@owner:baron
@describe:muid 月活muid中有oiid的量
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
create table if not exists mobdi_muid_dashboard.oiid_in_muid_mau_android_by_dws_device_sdk_run_master_di(
oiid_mau_his bigint '月活muid中有oiid的量(历史采集到过oiid就算)',
oiid_mau_now bigint '月活muid中有oiid的量(当月采集到的才算)'
)comment '月活muid中有oiid的量'
partitioned by (day string comment'日期')
stored as orc;
!
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.groupby.skewindata=true;
set hive.map.aggr=true;
set mapred.task.timeout=1800000;
insert overwrite table mobdi_muid_dashboard.oiid_in_muid_mau_android_by_dws_device_sdk_run_master_di partition(day='$day')
select count(*) as oiid_mau_his,sum(if(oiid_ltm >= '$firstDayOfMonth' and oiid_ltm <= '$day',1,0)) as oiid_mau_now
from
(select a.device, from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') as oiid_ltm
from
mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a
inner join
(
  select device, max(oiid_ltm_split) as oiid_ltm
  from
  (
    select device, oiid_ltm
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day'
  ) as m
  lateral view explode(split(oiid_ltm, ',')) mytable as oiid_ltm_split
  group by device
) as b
on a.device = b.device
)c;
"

