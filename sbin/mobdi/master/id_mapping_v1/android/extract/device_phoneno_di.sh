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
dws_device_phoneno_di=ex_log.dws_device_phoneno_di


HADOOP_USER_NAME=dba hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function extract_phone_num2 as 'com.youzu.mob.java.udf.PhoneNumExtract2';
create temporary function extract_phone_num3 as 'com.youzu.mob.java.udf.PhoneNumExtract3';
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';
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


insert overwrite table $dws_device_phoneno_di partition(day='$insert_day')
select
  device,
  concat_ws(',', collect_list(phoneno)) as phone,
  concat_ws(',', collect_list(phoneno_tm)) as phone_tm
from
(
    select
      log_info_jh.device as device,
      if (length(phoneno)=0, null, phoneno) as phoneno,
      cast(if(length(trim(phoneno))=0 or phoneno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as phoneno_tm
    from
    (
      select
          muid as device,
          case
            when lower(trim(phoneno)) rlike '[0-9a-f]{32}' then ''
            when length(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialno), trim(cast(carrier as string)),trim(imsi))), ',')[0]) = 17
            then string_sub_str(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialno), trim(cast(carrier as string)), trim(imsi))), ',')[0])
            else ''
          end as phoneno,
          serdatetime
      from $log_device_info_jh
      where dt ='$insert_day' and plat = 1 and muid is not null and length(trim(muid))=40 and phoneno is not null and length(phoneno)>0
    ) log_info_jh
) tt_jh
where phoneno is not null and length(phoneno)>0
group by device
"

# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_phoneno_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_phoneno_di drop if exists partition ($old_version)"
done
