#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';


add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function EXTRACT_PHONE_NUM as 'com.youzu.mob.java.udf.PhoneNumExtract2';

drop table if exists mobdi_test.gai_external_mac;
create table mobdi_test.gai_external_mac stored as orc as
select a.device, a.mac, a.mac_ltm, b.ext_phoneno, b.ext_phoneno_ltm
from 
(
  select device, lower(trim(exp_mac)) as mac, exp_mac_ltm as mac_ltm 
  from mobdi_test.gai_jh_mac_all
  lateral view explode_tags(concat(mac, '=', mac_ltm)) mytable as exp_mac, exp_mac_ltm
) as a 
inner join 
(
  select lower(trim(owner_data)) as mac, 
  split(EXTRACT_PHONE_NUM(trim(ext_data)), ',')[0] as ext_phoneno,
  cast(unix_timestamp(processtime, 'yyyyMMdd') as string) as ext_phoneno_ltm
  from dm_mobdi_mapping.ext_phone_mapping_incr 
  where type in ('mac_phone')
) as b 
on a.mac = b.mac
group by a.device, a.mac, a.mac_ltm, b.ext_phoneno, b.ext_phoneno_ltm;
"