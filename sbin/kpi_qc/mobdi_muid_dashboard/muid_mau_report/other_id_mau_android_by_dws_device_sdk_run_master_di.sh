#!/bin/bash

set -e -x

: '
@owner:baron
@describe:oiid,ieid,pid,isid月活
@projectName:mobdi_muid_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1
firstDayOfMonth=`date -d ${day} +%Y%m01`
yearhalf_days=`date -d "$day -540 days" +%Y%m%d`

:<<!
create table if not exists mobdi_muid_dashboard.other_id_mau_android_by_dws_device_sdk_run_master_di(
oiid_mau_match_history bigint comment 'oiid月活 匹配历史全量',
oiid_mau_match_yearhalf bigint comment'oiid月活 匹配近一年半',
ieid_mau_match_history bigint comment 'ieid月活 匹配历史全量',
ieid_mau_match_yearhalf bigint comment'ieid月活 匹配近一年半',
pid_mau_match_history bigint comment 'pid月活 匹配历史全量',
pid_mau_match_yearhalf bigint comment'pid月活 匹配近一年半',
isid_mau_match_history bigint comment 'isid月活 匹配历史全量',
isid_mau_match_yearhalf bigint comment'isid月活 匹配近一年半',
)comment 'oiid,ieid,pid,isid月活'
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
insert overwrite table mobdi_muid_dashboard.other_id_mau_android_by_dws_device_sdk_run_master_di partition(day='$day')
select 
oiid_mau_match_history,oiid_mau_match_yearhalf,ieid_mau_match_history,ieid_mau_match_yearhalf,pid_mau_match_history,pid_mau_match_yearhalf,isid_mau_match_history,isid_mau_match_yearhalf
from 
(
    select count(*) as oiid_mau_match_history
    from 
    (
            select mytable.oiid
            from
            (select a.oiid
            from 
                (select device, oiid
                from dim_mobdi_mapping.dim_device_oiid_merge_df
                where day = '$day' and length(oiid) > 0
                )a
                left semi join 
                mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre b 
                on a.device = b.device
            )c
        lateral view explode(split(c.oiid, ',')) mytable as oiid
        where length(mytable.oiid) > 0
        group by mytable.oiid
    ) as d
)a1
join
(
select count(*) as oiid_mau_match_yearhalf
from 
(
    select oiid
    from
        (
        select b.oiid_concat
        from 
        (select device, concat(oiid, '=', oiid_ltm) as oiid_concat
        from dim_mobdi_mapping.dim_device_oiid_merge_df
        where day = '$day' and length(oiid) > 0
        )b
        left semi join 
        mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a 
        on a.device = b.device 
        )tmp
    lateral view explode_tags(oiid_concat) mytable as oiid, oiid_ltm
    where from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by oiid
) as c
)a2
join
(
    select count(*) as ieid_mau_match_history
    from 
    (
            select mytable.ieid
            from
            (select a.ieid
            from 
                (select device, ieid
                from dim_mobdi_mapping.dim_device_ieid_merge_df
                where day = '$day' and length(ieid) > 0
                )a
                left semi join 
                mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre b 
                on a.device = b.device
            )c
        lateral view explode(split(c.ieid, ',')) mytable as ieid
        where length(mytable.ieid) > 0
        group by mytable.ieid
    ) as d
)a3
join
(
select count(*) as ieid_mau_match_yearhalf
from 
(
    select ieid
    from
        (
        select b.ieid_concat
        from 
        (select device, concat(ieid, '=', ieid_ltm) as ieid_concat
        from dim_mobdi_mapping.dim_device_ieid_merge_df
        where day = '$day' and length(ieid) > 0
        )b
        left semi join 
        mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a 
        on a.device = b.device 
        )tmp
    lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
    where from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by ieid
) as c
)a4
join
(
    select count(*) as pid_mau_match_history
    from 
    (
            select mytable.pid
            from
            (select a.pid
            from 
                (select device, pid
                from dim_mobdi_mapping.dim_device_pid_merge_df
                where day = '$day' and length(pid) > 0
                )a
                left semi join 
                mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre b 
                on a.device = b.device
            )c
        lateral view explode(split(c.pid, ',')) mytable as pid
        where length(mytable.pid) > 0
        group by mytable.pid
    ) as d
)a5
join
(
select count(*) as pid_mau_match_yearhalf
from 
(
    select pid
    from
        (
        select b.pid_concat
        from 
        (select device, concat(pid, '=', pid_ltm) as pid_concat
        from dim_mobdi_mapping.dim_device_pid_merge_df
        where day = '$day' and length(pid) > 0
        )b
        left semi join 
        mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a 
        on a.device = b.device 
        )tmp
    lateral view explode_tags(pid_concat) mytable as pid, pid_ltm
    where from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by pid
) as c
)a6
join
(
    select count(*) as isid_mau_match_history
    from 
    (
            select mytable.isid
            from
            (select a.isid
            from 
                (select device, isid
                from dim_mobdi_mapping.dim_device_isid_merge_df
                where day = '$day' and length(isid) > 0
                )a
                left semi join 
                mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre b 
                on a.device = b.device
            )c
        lateral view explode(split(c.isid, ',')) mytable as isid
        where length(mytable.isid) > 0
        group by mytable.isid
    ) as d
)a7
join
(
select count(*) as isid_mau_match_yearhalf
from 
(
    select isid
    from
        (
        select b.isid_concat
        from 
        (select device, concat(isid, '=', isid_ltm) as isid_concat
        from dim_mobdi_mapping.dim_device_isid_merge_df
        where day = '$day' and length(isid) > 0
        )b
        left semi join 
        mobdi_muid_dashboard.muid_mau_android_by_dws_device_sdk_run_master_di_pre a 
        on a.device = b.device 
        )tmp
    lateral view explode_tags(isid_concat) mytable as isid, isid_ltm
    where from_unixtime(cast(isid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by isid
) as c
)a8;
"
