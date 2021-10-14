#!/bin/bash
: '
@owner:luost
@describe:大、小众app活跃度标签
@projectName:mobdi
'

set -x -e

if [ $# -ne 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <timewindow>"
    exit 1
fi

day=$1
timewindow=$2

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di

#输出表
#label_l1_anticheat_device_cnt=dm_mobdi_report.label_l1_anticheat_device_cnt

function device_dzapp(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
SET mapreduce.job.queuename=root.yarn_mobfin.mobfin;
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

with device_pkg_table as (
    select apppkg,deviceid,count(1) as activecnt
    from 
    (
        select apppkg,deviceid,clienttime,day
        from
        (
            select apppkg,deviceid,cast(unix_timestamp(clienttime) as string) as clienttime,day
            from $dwd_pv_sec_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and apppkg is not null
            and apppkg <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0) 

            union all

            select apppkg,deviceid,substring(clienttime, 1, 10) as clienttime,day
            from $dwd_log_run_new_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and apppkg is not null
            and apppkg <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        )a
        group by apppkg,deviceid,clienttime,day
    )b
    group by apppkg,deviceid,day
)

insert overwrite table $label_l1_anticheat_device_cnt partition(day = '$day',timewindow = '$timewindow',flag = '7')
select deviceid as device,sum(cnt) as cnt
from 
(
    select deviceid,if(pkgcnt/$timewindow > 500000,activecnt,0) as cnt
    from 
    (
        select apppkg,deviceid,activecnt,count(1) over(partition by apppkg) as pkgcnt
        from device_pkg_table
    )a
)b
group by deviceid;
"
}

function device_xzapp(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
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

with device_pkg_table as (
    select apppkg,deviceid,count(1) as activecnt
    from 
    (
        select apppkg,deviceid,clienttime,day
        from
        (
            select apppkg,deviceid,cast(unix_timestamp(clienttime) as string) as clienttime,day
            from $dwd_pv_sec_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and apppkg is not null
            and apppkg <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)

            union all

            select apppkg,deviceid,substring(clienttime, 1, 10) as clienttime,day
            from $dwd_log_run_new_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and apppkg is not null
            and apppkg <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        )a
        group by apppkg,deviceid,clienttime,day
    )b
    group by apppkg,deviceid,day
)

insert overwrite table $label_l1_anticheat_device_cnt partition(day = '$day',timewindow = '$timewindow',flag = '8')
select deviceid as device,sum(cnt) as cnt
from 
(
    select deviceid,if(pkgcnt/$timewindow < 10000,activecnt,0) as cnt
    from 
    (
        select apppkg,deviceid,activecnt,count(1) over(partition by apppkg) as pkgcnt
        from device_pkg_table
    )a
)b
group by deviceid;
"
}

device_dzapp $timewindow

device_xzapp $timewindow
