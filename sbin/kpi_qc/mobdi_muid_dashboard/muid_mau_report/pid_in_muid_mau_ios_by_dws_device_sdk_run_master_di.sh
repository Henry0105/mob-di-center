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
create table if not exists mobdi_muid_dashboard.pid_in_muid_mau_ios_by_dws_device_sdk_run_master_di(
pid_mau_his bigint comment '月活muid中有pid的量(历史采集到过pid就算)',
pid_mau_now bigint comment '月活muid中有pid的量(当月采集到的才算)'
)comment 'ios版本月活muid中有pid的量'
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
insert overwrite table mobdi_muid_dashboard.pid_in_muid_mau_ios_by_dws_device_sdk_run_master_di partition(day='$day')
select count(*) as pid_mau_his,sum(if(pid_ltm >= '$firstDayOfMonth' and pid_ltm <= '$day',1,0)) as pid_mau_now
from
(select a.device, from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') as pid_ltm
from 
mobdi_muid_dashboard.muid_mau_ios_by_dws_device_sdk_run_master_di_pre a 
inner join 
(
  select device, max(pid_ltm_split) as pid_ltm
  from
  (
    select device, pid_ltm
    from dm_mobdi_mapping.ios_id_mapping_sec_df
    where version = '${day}.1001' and length(pid) > 0
  ) as m 
  lateral view explode(split(pid_ltm, ',')) mytable as pid_ltm_split
  group by device
) as b 
on a.device = b.device
)c;
"
