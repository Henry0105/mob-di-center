#!/bin/bash

set -e -x


HADOOP_USER_NAME=dba hive -e "
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


insert overwrite table ex_log.android_imei_phone_exchange partition (day='20201007')
select imei,pid as phone,unix_timestamp('2020-10-07 00:00:00') phone_ltm from ex_log.ieid_pid_mapping_jimi where pid_ieidcnt<=3 and ieid_pidcnt<=3;

insert overwrite table ex_log.android_imei_phone_exchange partition (day='20201212')
select ieid as imei,pid as phone,unix_timestamp('20201212 13:01:00','yyyyMMdd HH:mm:ss') phone_ltm from ex_log.ieid_pid_mapping_jimi_1225 where pid_ieidcnt<=3 and ieid_pidcnt<=3;

insert overwrite table ex_log.android_imei_phone_exchange partition (day='20210318')
select ieid as imei,pid as phone,'1616043660' phone_ltm from ex_log.ieid_pid_mapping_jimi_0318 where pid_ieidcnt<=3 and ieid_pidcnt<=3;

insert overwrite table ex_log.android_imei_phone_exchange partition (day='20210520')
select ieid as imei,pid as phone,'1621440000' phone_ltm from ex_log.ieid_pid_mapping_jimi_0525 where pid_ieidcnt<=3 and ieid_pidcnt<=3;

insert overwrite table ex_log.android_imei_phone_exchange partition (day='20210811')
select ieid as imei,pid as phone,unix_timestamp('20210811 00:00:00','yyyyMMdd HH:mm:ss') phone_ltm from ex_log.ieid_pid_mapping_jimi_0722 where pid_ieidcnt<=3 and ieid_pidcnt<=3;

insert overwrite table ex_log.android_imei_phone_exchange partition (day='20211031')
select ieid as imei,pid as phone,unix_timestamp('20211031 00:00:00','yyyyMMdd HH:mm:ss') phone_ltm from ex_log.ieid_pid_mapping_jimi_20211022 where pid_ieidcnt<=2 and ieid_pidcnt<=2;

insert overwrite table ex_log.android_imei_phone_exchange partition (day='20211224')
select ieid as imei,pid as phone,unix_timestamp('20211224 00:00:00','yyyyMMdd HH:mm:ss') phone_ltm from ex_log.ieid_pid_mapping_jimi_20211224 where pid_ieidcnt<=2 and ieid_pidcnt<=2;

"
