#!/bin/sh

set -x -e

: '
@owner:hushk,luost
@describe:android_pid_每日增量表
@projectName:mobdi
@BusinessName:id_mapping
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1

#input
log_device_pid_dedup=dm_mobdi_master.dwd_log_device_pid_sec_di
android_id_mapping_incr=dm_mobdi_topic.dws_id_mapping_android_sec_di
mobauth_operator_login=dm_mobdi_master.dwd_mobauth_operator_login_sec_di
mobauth_operator_auth=dm_mobdi_master.dwd_mobauth_operator_auth_sec_di
mobauth_pvlog=dm_mobdi_master.dwd_mobauth_pvlog_sec_di

#output
pid_mapping_incr=dm_mobdi_topic.dws_pid_mapping_sec_di

hive -e "
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5500m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5500m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5500m' -XX:+UseG1GC;
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

with
sms_pidno as (
    select devices_shareid as device,
           concat_ws(',',collect_list(pid)) as pid,
           concat_ws(',',collect_list(cast(if(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') is not null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),unix_timestamp('$insert_day','yyyyMMdd')) as string))) as pid_tm
    from
    (
        select devices_shareid,
               pid,
               max(serdatetime) as serdatetime
        from $log_device_pid_dedup
        where day = '$insert_day'
        and devices_plat = 1
        and zone in ('852','853','886','86', '1', '7', '81', '82')
        and length(trim(devices_shareid)) = 40
        and devices_shareid = regexp_extract(devices_shareid, '[0-9a-f]{40}', 0)
        and dt = '$insert_day'
        and pid is not null
        and pid != ''
        group by devices_shareid,pid
    )t
    group by devices_shareid
),

mobauth_pid_v as (
    select device,
           concat_ws(',', collect_list(pid)) as mobauth_pid,
           concat_ws(',', collect_list(datetime)) as mobauth_pid_tm
    from
    (
        select device,
               pid,
               cast(max(datetime) as string) as datetime
        from
        (
            select deviceid as device,
                   pid,
                   cast(max(datetime)/1000 as bigint) as datetime
            from $mobauth_operator_login
            where day = '$insert_day'
            and pid != ''
            and pid is not null
            and deviceid != ''
            and deviceid is not null
            and deviceid = regexp_extract(deviceid, '[0-9a-f]{40}', 0)
            and plat = 1
            group by deviceid,pid

            union all

            select t2.deviceid as device,pid,datetime
            from
            (
                select duid,
                       pid,
                       cast(max(datetime)/1000 as bigint) as datetime
                from $mobauth_operator_auth
                where plat = 1
                and duid is not null
                and pid is not null
                and length(duid) = 40
                and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
                and day = '$insert_day'
                group by duid,pid
            ) t1
            inner join
            (
                select deviceid,
                       duid
                from $mobauth_pvlog
                where plat = 1
                and deviceid is not null
                and duid is not null
                and length(deviceid) = 40
                and length(duid) = 40
                and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
                and deviceid = regexp_extract(deviceid, '[0-9a-f]{40}', 0)
                and day = '$insert_day'
                group by deviceid,duid
            ) t2
            on t1.duid = t2.duid
        )a group by device,pid
    )a group by device
),

pidno_isid as(
    select coalesce(c.device,d.device) as device,
           pidno,
           pidno_tm,
           ext_pidno,
           ext_pidno_tm,
           sms_pidno,
           sms_pidno_tm,
           isid,
           isid_tm,
           nvl(d.mobauth_pid,null) as mobauth_pid,
           nvl(d.mobauth_pid_tm,null) as mobauth_pid_tm
    from
    (
        select coalesce(a.device,b.device) as device,
               nvl(a.pidno,null) as pidno,
               nvl(a.pidno_tm,null) as pidno_tm,
               nvl(a.ext_pidno,null) as ext_pidno,
               nvl(a.ext_pidno_tm,null) as ext_pidno_tm,
               nvl(b.pid,null) as sms_pidno,
               nvl(b.pid_tm,null) as sms_pidno_tm,
               nvl(a.isid,null) as isid,
               nvl(a.isid_tm,null) as isid_tm
        from
        (
            select device,
                   pidno,
                   pidno_tm,
                   null as ext_pidno,
                   null as ext_pidno_tm,
                   isid,
                   isid_tm
            from $android_id_mapping_incr
            where day = '$insert_day'
            and device is not null
            and length(device)= 40
            and device = regexp_extract(device,'([a-f0-9]{40})', 0)
        ) a
        full join
        sms_pidno b on a.device = b.device
    )c
    full join
    mobauth_pid_v d on c.device = d.device
)

insert overwrite table $pid_mapping_incr partition(day='$insert_day',plat=1)
select device,
       pidno,
       pidno_tm,
       ext_pidno,
       ext_pidno_tm,
       sms_pidno,
       sms_pidno_tm,
       isid,
       isid_tm,
       null as ext_isid,
       null as ext_isid_tm,
       mobauth_pid,
       mobauth_pid_tm
from pidno_isid;
"
