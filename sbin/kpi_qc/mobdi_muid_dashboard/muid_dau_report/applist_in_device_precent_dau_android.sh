#!/bin/bash

set -e -x

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi
day=$1

HADOOP_USER_NAME=dba hive -e"
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.reducers.bytes.per.reducer=1073741824;

insert overwrite table mobdi_muid_dashboard.applist_in_three_id_precent_dau_android partition(day='$day',id_type='device')
select
applist_in_device,device_dau,applist_in_device/device_dau as applist_in_device_percent
from
(
select count(a.device) as applist_in_device
from
(
select device
from dm_mobdi_master.device_sdk_run_master_add_mdata_pv_view
where day = '$day'  and plat = '1'
group by device
) as a
left semi join
( select  device
from dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
where day = '$day' and plat = 1
group  by device
) b
on a.device = b.device
)a1
join
(
select count(*)as device_dau
from
(
select device
from dm_mobdi_master.device_sdk_run_master_add_mdata_pv_view
where day = '$day'  and plat = '1'
group by device
)a2
)d
;"

