#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance1;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3g';
SET hive.exec.parallel.thread.number=6;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';

drop table if exists mobdi_test.gai_mobauth;
create table mobdi_test.gai_mobauth stored as orc as
select device, phone, time
from (
  select device, string_sub_str(phone) as phone, cast(datetime as string) as time
  from mobdi_test.gai_device_phone_2020_pre 
  union all
  select device, string_sub_str(phone) as phone, cast(datetime as string) as time
  from mobdi_test.gai_device_phone_2020_after 
)a
group by device, phone, time;
"
