#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -e -v "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx6144m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set mapreduce.job.queuename=root.yarn_data_compliance;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function EXTRACT_PHONE_NUM as 'com.youzu.mob.java.udf.PhoneNumExtract2';

drop table if exists mobdi_test.gaizi_sms_phone;
create table mobdi_test.gaizi_sms_phone as
select device, phone, day, max(serdatetime) as serdatetime
from
(
  select muid as device, split(EXTRACT_PHONE_NUM(concat('+', zone, ' ', phone)), ',')[0] as phone, coalesce(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'), cast(substr(createat, 1, 10) as bigint)) as serdatetime, coalesce(split(serdatetime, ' ')[0], from_unixtime(cast(substr(createat, 1, 10) as bigint), 'yyyy-MM-dd')) as day
  from dm_smssdk_master.log_device_phone_dedup
  where day = '20220228' and hour = '01'
  and devices_plat = 1 and zone in ('852','853','886','86', '1', '7', '81', '82')
  and length(trim(muid)) = 40
  and phone rlike '^[1][3-8]\\\d{9}$|^([6|9])\\\d{7}$|^[0][9]\\\d{8}$|^[6]([8|6])\\\d{5}$'
) as t
where length(phone) = 17
group by device, phone, day;
"