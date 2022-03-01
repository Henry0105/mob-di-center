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

# output
dws_device_isid_di=dm_mobdi_topic.dws_device_isid_di


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


insert overwrite table $dws_device_isid_di partition(day='$insert_day')
select
  device,
  concat_ws(',', collect_list(isid)) as isid,
  concat_ws(',', collect_list(isid_tm)) as isid_tm
from
(
      select
          muid as device,
          isid,
          cast(if(length(isid)=0 or isid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as isid_tm
      from $dwd_log_device_info_jh_sec_di
      where day ='$insert_day' and plat = 1 and  muid is not null and length(trim(muid))=40 and isid is not null and length(isid)>0
) tt_jh
group by device
"

# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_isid_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_isid_di drop if exists partition ($old_version)"
done