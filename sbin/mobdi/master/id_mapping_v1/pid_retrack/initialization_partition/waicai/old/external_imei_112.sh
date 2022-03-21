#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=20480;
set mapreduce.map.java.opts='-Xmx18g' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx18g';
set mapreduce.reduce.memory.mb=20480;
set mapreduce.reduce.java.opts='-Xmx18g' -XX:+UseG1GC;
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


drop table if exists mobdi_test.gai_external_imei_112;
create table mobdi_test.gai_external_imei_112 stored as orc as
select a.device, a.imei, a.imei_ltm, b.ext_phoneno, b.ext_phoneno_ltm
from 
(
  select device, lower(trim(exp_imei)) as imei, exp_imei_ltm as imei_ltm 
  from mobdi_test.gai_jh_imei_all
  lateral view explode_tags(concat(imei, '=', imei_ltm)) mytable as exp_imei, exp_imei_ltm  
  where cast(conv(substring(device,0,1), 16, 10) as int)=12
) as a 
inner join 
(
  select * from mobdi_test.gai_external_old
) as b 
on substring(a.imei, 1, 14) = substring(b.imei, 1, 14)
group by a.device, a.imei, a.imei_ltm, b.ext_phoneno, b.ext_phoneno_ltm;
"