#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_mobdi.dba;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function EXTRACT_PHONE_NUM as 'com.youzu.mob.java.udf.PhoneNumExtract2';

drop table if exists mobdi_test.gai_external_imei_3;
create table mobdi_test.gai_external_imei_3 stored as orc as
select a.device, a.imei, a.imei_ltm, b.ext_phoneno, b.ext_phoneno_ltm
from 
(
  select device, lower(trim(exp_imei)) as imei, exp_imei_ltm as imei_ltm 
  from mobdi_test.gai_jh_imei_all
  lateral view explode_tags(concat(imei, '=', imei_ltm)) mytable as exp_imei, exp_imei_ltm
) as a 
inner join 
(
  select lower(trim(imei)) as imei,phone as ext_phoneno,phone_ltm as ext_phoneno_ltm
  from ex_log.android_imei_phone_exchange 
) as b 
on substring(a.imei, 1, 14) = substring(b.imei, 1, 14)
group by a.device, a.imei, a.imei_ltm, b.ext_phoneno, b.ext_phoneno_ltm;
"
