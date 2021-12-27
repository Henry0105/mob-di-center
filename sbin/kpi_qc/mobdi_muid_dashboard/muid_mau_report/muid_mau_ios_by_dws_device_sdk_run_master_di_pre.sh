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

#muid月活安卓前置
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
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.map.aggr=true;
set mapred.task.timeout=1800000;
drop table if exists mobdi_muid_dashboard.muid_mau_ios_by_dws_device_sdk_run_master_di_pre;
create table mobdi_muid_dashboard.muid_mau_ios_by_dws_device_sdk_run_master_di_pre stored as orc as 
select device
from 
(
     select deviceid as device 
     from dm_mobdi_master.dwd_log_run_new_di
     where day <= '$day' and day >= '$firstDayOfMonth' and plat = 2
     group by deviceid
     union all
     select deviceid as device
     from dm_mobdi_master.dwd_app_runtimes_stats_sec_di
     where day <= '$day' and day >= '$firstDayOfMonth' and plat = 2
     group by deviceid
     union all
     select deviceid as device
     from dm_mobdi_master.dwd_pv_sec_di
     where day <= '$day' and day >= '$firstDayOfMonth' and plat = 2
     group by deviceid
     union all
     select deviceid as device
     from dm_mobdi_master.dwd_mdata_nginx_pv_di
     where day <= '$day' and day >= '$firstDayOfMonth' and plat = 2
     group by deviceid
)a
group by device
"

