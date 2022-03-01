#!/bin/bash

set -e -x

# input
android_imei_phone_exchange=ex_log.android_imei_phone_exchange

# output
i_p=mobdi_test.i_p


HADOOP_USER_NAME=dba hive -v -e "
SET hive.auto.convert.join=true;
SET hive.map.aggr=false;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx7370m';
set mapreduce.child.map.java.opts='-Xmx7370m';
set mapreduce.reduce.memory.mb=15360;
set mapreduce.reduce.java.opts='-Xmx13900m' -XX:+UseG1GC;
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;
set mapred.task.timeout=1800000;
set mapreduce.job.queuename=root.yarn_data_compliance;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';




---step1  14,15位的,都截取为 14位
drop table if exists $i_p;
create table $i_p stored as orc as
select
     case when trim(imei) rlike '^[0-9]{14,15}$' and  luhn_checker(substring(trim(imei) ,1,14)) then substring(imei,1,14)
          else imei
     end as imei14 ,
     phone ,
     max(phone_ltm) phone_ltm
from
( select imei,phone,phone_ltm from $android_imei_phone_exchange) as s_t
group by case when trim(imei) rlike '^[0-9]{14,15}$' and luhn_checker(substring(trim(imei) ,1,14)) then substring(imei,1,14)
              else imei
          end,phone;
"
