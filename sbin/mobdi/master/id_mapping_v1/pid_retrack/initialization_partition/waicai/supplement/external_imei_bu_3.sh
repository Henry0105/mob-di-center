#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function EXTRACT_PHONE_NUM as 'com.youzu.mob.java.udf.PhoneNumExtract2';

drop table if exists mobdi_test.gai_external_imei_bu_3;
create table mobdi_test.gai_external_imei_bu_3 stored as orc as
select a.device, a.imei, a.imei_ltm, b.ext_phoneno, b.ext_phoneno_ltm
from 
(
  select device, lower(trim(exp_imei)) as imei, exp_imei_ltm as imei_ltm
  from mobdi_test.gai_jh_imei_all
  lateral view explode_tags(concat(imei, '=', imei_ltm)) mytable as exp_imei, exp_imei_ltm
  where cast(conv(substring(device,0,1), 16, 10) as int)%4=3
) as a 
inner join 
(
  select lower(trim(ext_data)) as imei,
  split(EXTRACT_PHONE_NUM(trim(owner_data)), ',')[0] as ext_phoneno,
  cast(unix_timestamp(processtime, 'yyyyMMdd') as string) as ext_phoneno_ltm
  from dm_mobdi_mapping.ext_phone_mapping_incr
  where day='20190611' and type = 'phone_imei'
  group by lower(trim(ext_data)),split(EXTRACT_PHONE_NUM(trim(owner_data)), ',')[0],cast(unix_timestamp(processtime, 'yyyyMMdd') as string)
) as b 
on substring(a.imei, 1, 14) = substring(b.imei, 1, 14)
group by a.device, a.imei, a.imei_ltm, b.ext_phoneno, b.ext_phoneno_ltm;
"