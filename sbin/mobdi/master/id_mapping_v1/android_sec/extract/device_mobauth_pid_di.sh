#!/bin/bash

set -e -x

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

insert_day=$1

# input
mobauth_operator_login=dm_mobdi_master.dwd_mobauth_operator_login_sec_di
mobauth_operator_auth=dm_mobdi_master.dwd_mobauth_operator_auth_sec_di
mobauth_pvlog=dm_mobdi_master.dwd_mobauth_pvlog_sec_di

# output
dws_device_mobauth_pid_di=dm_mobdi_topic.dws_device_mobauth_pid_di


HADOOP_USER_NAME=dba hive -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;


insert overwrite table $dws_device_mobauth_pid_di partition(day='$insert_day')
select
  device,
  concat_ws(',', collect_list(pid)) as pid,
  concat_ws(',', collect_list(datetime)) as pid_tm
from
(
    select device,
           pid,
           cast(max(datetime) as string) as datetime
    from
    (
        select muid as device,
               pid,
               cast(max(datetime)/1000 as bigint) as datetime
        from $mobauth_operator_login
        where day = '$insert_day'
        and pid != ''
        and pid is not null
        and muid != ''
        and muid is not null
        and muid = regexp_extract(muid, '[0-9a-f]{40}', 0)
        and plat = 1
        group by muid,pid

        union all

        select t2.device as device,pid,datetime
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
            select muid as device,
                   duid
            from $mobauth_pvlog
            where plat = 1
            and muid is not null
            and duid is not null
            and length(muid) = 40
            and length(duid) = 40
            and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
            and muid = regexp_extract(muid, '[0-9a-f]{40}', 0)
            and day = '$insert_day'
            group by muid,duid
        ) t2
        on t1.duid = t2.duid
    )a group by device,pid
) tt_jh
where pid is not null and length(pid)>0
group by device
"


# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_mobauth_pid_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_mobauth_pid_di drop if exists partition ($old_version)"
done