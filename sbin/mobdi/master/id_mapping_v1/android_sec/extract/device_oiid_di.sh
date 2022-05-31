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
dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di
dws_device_duid_mapping_new_sec_par=dm_mobdi_topic.dws_device_duid_mapping_new_sec_par
dwd_awaken_dfl_sec_di=dm_mobdi_master.dwd_awaken_dfl_sec_di

# output
dws_device_oiid_di=dm_mobdi_topic.dws_device_oiid_di


HADOOP_USER_NAME=dba hive -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx4608m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx4608m';
set mapreduce.reduce.memory.mb=14336;
set mapreduce.reduce.java.opts='-Xmx11468m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;


insert overwrite table $dws_device_oiid_di partition(day='$insert_day')
select
  device,
  concat_ws(',', collect_list(oiid)) as oiid,
  concat_ws(',', collect_list(oiid_tm)) as oiid_tm
from
(
    select
        device,
        oiid,
        max(oiid_tm) as oiid_tm
    from
    (
      select
        trim(lower(muid)) as device,
        oiid,
        unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as oiid_tm,
        '' as duid
      from $dwd_log_device_info_jh_sec_di
      where day='$insert_day'
      and oiid is not null
      and lower(oiid) not in ('','null')
      and plat=1
      and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
      and trim(muid) != '0000000000000000000000000000000000000000'

      union all

      select
        b.device as device,
        oiid,
        a.oiid_tm as oiid_tm,
        a.duid as duid
      from
      (
        select
               get_json_object(extra, '$.cnt_fids.fids.oiid') as oiid,
               substr(serdatetime,0,10) as oiid_tm,
               lower(trim(duid)) as duid
        from $dwd_awaken_dfl_sec_di
        where day='$insert_day'
        and get_json_object(extra, '$.cnt_fids.fids.oiid') is not null
        and lower(trim(get_json_object(extra, '$.cnt_fids.fids.oiid'))) not in ('','null')
        and duid is not null
        and lower(trim(duid)) not in ('','null')
      ) a
      left join
      (
        select
            device,
            duid,
            processtime
        from
        (
          select trim(lower(device)) as device,
                 trim(lower(duid)) as duid,
                 processtime,
                 row_number() over (partition by duid order by processtime desc) as rn
          from $dws_device_duid_mapping_new_sec_par
          where day='$insert_day' and length(trim(device))>0
          and length(trim(duid))>0
          and trim(lower(device)) rlike '^[a-f0-9]{40}$'
          and trim(device) != '0000000000000000000000000000000000000000'
          and plat=1
        )m where rn=1
      ) b
      on a.duid = b.duid
    ) tt
    where device is not null
    group by device,oiid
) a
where oiid is not null and length(oiid)>0
group by device
"


# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_oiid_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_oiid_di drop if exists partition ($old_version)"
done
