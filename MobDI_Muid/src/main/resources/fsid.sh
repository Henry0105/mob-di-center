#!/bin/bash
set -x -e

#生成20201101-20211101的duid fsid使用的日期参数是20211106,如果要生成之前的日期的fsid,需要用小于20211106的参数,要大于udf的starttime
hive -e "
add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF';
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
set mapreduce.job.queuename=root.yarn_etl.etl;
set mapreduce.map.java.opts=-Xmx20000m;
set mapreduce.map.memory.mb=19000;
set mapreduce.reduce.java.opts=-Xmx15000m;
set mapreduce.reduce.memory.mb=14000;
insert overwrite table dm_mid_master.duid_sfid_mapping partition(version='2019-2021')
select coalesce(a.duid,b.duid) duid,coalesce(b.sfid,fsid('20211104')) sfid from (
select duid from dm_mid_master.pkg_it_duid_par_tmp
where day in ('201911','201912','202001','202002','202003','202004','202005','202006','202007','202008','202009','202010')
and duid is not null and trim(duid) <> '' group by duid
) a
full join
(select duid,sfid from dm_mid_master.duid_sfid_mapping where version='2020-2021') b on a.duid = b.duid
"
