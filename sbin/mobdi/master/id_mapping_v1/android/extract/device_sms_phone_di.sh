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
log_device_phone_dedup=dm_smssdk_master.log_device_phone_dedup

# output   更新逻辑,保留最近一周数据，其它按照月份第一天保存
dws_device_sms_phone_di=ex_log.dws_device_sms_phone_di


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


insert overwrite table $dws_device_sms_phone_di partition(day='$insert_day')
select
    muid as device,
    concat_ws(',',collect_list(string_sub_str(phone))) as phone,
    concat_ws(',',collect_list(cast( if(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') is not null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),unix_timestamp('$insert_day','yyyyMMdd')) as string))) as phone_tm
from(
    select
        muid,
        case
          when lower(trim(phone)) rlike '[0-9a-f]{32}' then ''
          when zone in ('852','853','886','86', '1', '7', '81', '82') then split(extract_phone_num2(concat('+', zone, ' ', phone)), ',')[0]
        else ''
        end as phone,
        serdatetime
    from $log_device_phone_dedup
    where day='$insert_day' and hour='01'
    and devices_plat=1
    and length(trim(muid)) = 40
) t
where length(phone)=17
group by muid
"

# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_sms_phone_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_sms_phone_di drop if exists partition ($old_version)"
done
