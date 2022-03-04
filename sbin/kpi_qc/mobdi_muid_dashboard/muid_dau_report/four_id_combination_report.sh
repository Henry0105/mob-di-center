#!/bin/bash

set -e -x
if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi
day=$1
HADOOP_USER_NAME=dba hive -e"
set hive.exec.parallel=true;
SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx6144M' -XX:+UseG1GC;;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144M';
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;

with muid_dau_android_temp as (
select
muid_dau_android
from mobdi_muid_dashboard.muid_dau_report_by_dws_device_sdk_run_master_di 
where day=$day
),
applist_y_oiid_y_ieid_y_asid_y_temp as (
select 'applist_y_oiid_y_ieid_y_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid','oiid_in_muid','applist_in_muid')
group by device 
having count(*)=4
)a
),
applist_y_oiid_y_ieid_y_asid_n_temp as (
select 'applist_y_oiid_y_ieid_y_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid','oiid_in_muid','applist_in_muid')
group by device 
having count(*)=3
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_y_oiid_y_ieid_n_asid_y_temp as (
select 'applist_y_oiid_y_ieid_n_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','oiid_in_muid','applist_in_muid')
group by device 
having count(*)=3
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_y_oiid_n_ieid_y_asid_y_temp as (
select 'applist_y_oiid_n_ieid_y_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid','applist_in_muid')
group by device 
having count(*)=3
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('oiid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_y_oiid_y_ieid_n_asid_n_temp as (
select 'applist_y_oiid_y_ieid_n_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('oiid_in_muid','applist_in_muid')
group by device 
having count(*)=2
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_y_oiid_n_ieid_y_asid_n_temp as (
select 'applist_y_oiid_n_ieid_y_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid','applist_in_muid')
group by device 
having count(*)=2
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','oiid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_y_oiid_n_ieid_n_asid_y_temp as (
select 'applist_y_oiid_n_ieid_n_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','applist_in_muid')
group by device 
having count(*)=2
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid','oiid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_y_oiid_n_ieid_n_asid_n_temp as (
select 'applist_y_oiid_n_ieid_n_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('applist_in_muid')
group by device 
having count(*)=1
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid','oiid_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_y_ieid_y_asid_y_temp as (
select 'applist_n_oiid_y_ieid_y_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid','oiid_in_muid')
group by device 
having count(*)=3
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_y_ieid_y_asid_n_temp as (
select 'applist_n_oiid_y_ieid_y_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid','oiid_in_muid')
group by device 
having count(*)=2
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_y_ieid_n_asid_y_temp as (
select 'applist_n_oiid_y_ieid_n_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','oiid_in_muid')
group by device 
having count(*)=2
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_n_ieid_y_asid_y_temp as (
select 'applist_n_oiid_n_ieid_y_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid')
group by device 
having count(*)=2
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('oiid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_y_ieid_n_asid_n_temp as (
select 'applist_n_oiid_y_ieid_n_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('oiid_in_muid')
group by device 
having count(*)=1
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_n_ieid_y_asid_n_temp as (
select 'applist_n_oiid_n_ieid_y_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid')
group by device 
having count(*)=1
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','oiid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
applist_n_oiid_n_ieid_n_asid_y_temp as (
select 'applist_n_oiid_n_ieid_n_asid_y' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid')
group by device 
having count(*)=1
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('ieid_in_muid','oiid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
),
muid_when_applist_n_oiid_n_ieid_n_asid_n_temp as (
select 'muid_when_applist_n_oiid_n_ieid_n_asid_n' as name,count(*) as cnt
from 
(select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where  day='${day}' and plat=1 and muid_type='muid_dau'
)a
left join
(
select  device 
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day='${day}' and plat=1 and
muid_type in ('asid_in_muid','ieid_in_muid','oiid_in_muid','applist_in_muid')
group by device 
)b
on a.device=b.device
where b.device is null
)
insert overwrite table mobdi_muid_dashboard.id_combination_report partition(day=$day,id_type='four_id')
select 
name,cnt,round(cnt/muid_dau_android,4) as percent
from 
(select name,cnt
from applist_n_oiid_y_ieid_y_asid_y_temp
union all
select name,cnt
from applist_y_oiid_n_ieid_n_asid_n_temp
union all
select name,cnt
from applist_y_oiid_n_ieid_y_asid_y_temp
union all
select name,cnt
from applist_y_oiid_y_ieid_y_asid_n_temp
union all
select name,cnt
from applist_n_oiid_y_ieid_y_asid_n_temp
union all
select name,cnt
from applist_y_oiid_y_ieid_n_asid_n_temp
union all
select name,cnt
from muid_when_applist_n_oiid_n_ieid_n_asid_n_temp
union all
select name,cnt
from applist_n_oiid_y_ieid_n_asid_n_temp
union all
select name,cnt
from applist_y_oiid_y_ieid_n_asid_y_temp
union all
select name,cnt
from applist_n_oiid_n_ieid_n_asid_y_temp
union all
select name,cnt
from applist_y_oiid_y_ieid_y_asid_y_temp
union all
select name,cnt
from applist_y_oiid_n_ieid_y_asid_n_temp
union all
select name,cnt
from applist_n_oiid_n_ieid_y_asid_y_temp
union all
select name,cnt
from applist_y_oiid_n_ieid_n_asid_y_temp
union all
select name,cnt
from applist_n_oiid_n_ieid_y_asid_n_temp
union all
select name,cnt
from applist_n_oiid_y_ieid_n_asid_y_temp
)all
join muid_dau_android_temp 
;"
