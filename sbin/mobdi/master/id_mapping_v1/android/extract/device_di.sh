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
dws_device_imei_di=ex_log.dws_device_imei_di
dws_device_imsi_di=ex_log.dws_device_imsi_di
dws_device_mac_di=ex_log.dws_device_mac_di
dws_device_phone_di=ex_log.dws_device_phone_di
dws_device_serialno_di=ex_log.dws_device_serialno_di
dws_device_oaid_di=ex_log.dws_device_oaid_di


# output
dws_device_di=ex_log.dws_device_di


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

insert overwrite table $dws_device_di partition (day='$insert_day')
select
    device
from
(
  select device from $dws_device_imei_di where day='$insert_day' and device is not null and length(device)=40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  union all
  select device from $dws_device_imsi_di where day='$insert_day' and device is not null and length(device)=40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  union all
  select device from $dws_device_mac_di where day='$insert_day' and device is not null and length(device)=40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  union all
  select device from  $dws_device_phone_di where day='$insert_day' and device is not null and length(device)=40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  union all
  select device from  $dws_device_serialno_di where day='$insert_day' and device is not null and length(device)=40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  union all
  select device  from $dws_device_oaid_di where day='$insert_day' and device is not null and length(device)=40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) t group by device
"

# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_di drop if exists partition ($old_version)"
done