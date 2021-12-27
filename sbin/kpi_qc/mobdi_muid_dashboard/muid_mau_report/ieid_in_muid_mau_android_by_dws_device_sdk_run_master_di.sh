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
create table if not exists mobdi_muid_dashboard.ieid_in_muid_mau_android_by_dws_device_sdk_run_master_di(
ieid_mau_his bigint '月活muid中有ieid的量(历史采集到过ieid就算)',
ieid_mau_now bigint '月活muid中有ieid的量(当月采集到的才算)'
)comment '月活muid中有ieid的量'
partitioned by (day string comment'日期')
stored as orc;
!

HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3700m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3700m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx4g' -XX:+UseG1GC;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table mobdi_muid_dashboard.ieid_in_muid_mau_android_by_dws_device_sdk_run_master_di partition(day='$day')
select count(*) as ieid_mau_his,sum(if(ieid_ltm >= '$firstDayOfMonth' and ieid_ltm <= '$day',1,0)) as ieid_mau_now
from
(select a.device, from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') as ieid_ltm
from
mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a
inner join
(
  select device, max(ieid_ltm_split) as ieid_ltm
  from
  (
    select device, ieid_ltm
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day'
  ) as m
  lateral view explode(split(ieid_ltm, ',')) mytable as ieid_ltm_split
  group by device
) as b
on a.device = b.device
)c;
"
