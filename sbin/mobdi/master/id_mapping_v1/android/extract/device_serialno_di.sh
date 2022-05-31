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
log_device_info_jh=dw_sdk_log.log_device_info_jh

# output
dws_device_serialno_di=ex_log.dws_device_serialno_di


HADOOP_USER_NAME=dba hive -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5120m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5120m';
set mapreduce.reduce.memory.mb=14336;
set mapreduce.reduce.java.opts='-Xmx11468m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;


insert overwrite table $dws_device_serialno_di partition (day='$insert_day')
select
  device,
  concat_ws(',', collect_list(serialno)) as serialno,
  concat_ws(',', collect_list(serialno_tm)) as serialno_tm
from
(
    select
      log_info_jh.device as device,
      if (length(serialno)=0, null, serialno) as serialno,
      cast(if(length(serialno)=0 or serialno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as serialno_tm
    from
    (
      select
          muid as device,
          case
            when lower(trim(serialno)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or serialno is null then ''
            when lower(trim(serialno)) rlike '^[0-9a-z]{6,32}$' then lower(trim(serialno))
            else ''
          end as serialno,
          serdatetime
      from $log_device_info_jh
      where dt ='$insert_day' and plat=1 and muid is not null and length(trim(muid))=40 and serialno is not null and length(serialno)>0
    ) log_info_jh
) tt_jh where serialno is not null and length(serialno)>0
group by device
"

# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_serialno_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_serialno_di drop if exists partition ($old_version)"
done