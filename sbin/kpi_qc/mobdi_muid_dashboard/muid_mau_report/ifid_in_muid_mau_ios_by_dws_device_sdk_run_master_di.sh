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
create table if not exists mobdi_muid_dashboard.ifid_in_muid_mau_ios_by_dws_device_sdk_run_master_di(
ifid_mau_his bigint comment '月活muid中有ifid的量(历史采集到过ifid就算)',
ifid_mau_now bigint comment '月活muid中有ifid的量(当月采集到的才算)'
)comment 'ios版本月活muid中有ifid的量'
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
insert overwrite table mobdi_muid_dashboard.ifid_in_muid_mau_ios_by_dws_device_sdk_run_master_di partition(day='$day')
select count(*) as ifid_mau_his,sum(if(ifid_ltm >= '$firstDayOfMonth' and ifid_ltm <= '$day',1,0)) as ifid_mau_now
from
(select a.device, from_unixtime(cast(ifid_ltm as bigint), 'yyyyMMdd') as ifid_ltm
from 
mobdi_muid_dashboard.muid_mau_ios_by_dws_device_sdk_run_master_di_pre a 
inner join 
(
  select device, max(ifid_ltm_split) as ifid_ltm
  from
  (
    select device, ifid_ltm
    from dm_mobdi_mapping.ios_id_mapping_sec_df
    where version = '${day}.1001' and length(ifid) > 0
  ) as m 
  lateral view explode(split(ifid_ltm, ',')) mytable as ifid_ltm_split
  group by device
) as b 
on a.device = b.device
)c;
"
