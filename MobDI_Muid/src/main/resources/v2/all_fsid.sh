#!/bin/bash
set -x -e

tmp_db=dm_mid_master

duid_sfid_mapping=$tmp_db.duid_unid_mapping

id_source="$tmp_db.dwd_all_id_detail"

hive -e "
add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function sfid as 'com.mob.udf.HistorySnowflakeUDF';
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
set mapreduce.map.java.opts=-Xmx20000m;
set mapreduce.map.memory.mb=19000;
set mapreduce.reduce.java.opts=-Xmx15000m;
set mapreduce.reduce.memory.mb=14000;
insert overwrite table $duid_sfid_mapping partition(version='all')
select coalesce(a.duid,b.duid) duid,coalesce(b.sfid,sfid('20211019')) sfid
from (
select duid from $id_source where day='all' group by duid
) a
full join
(select duid,sfid from $duid_sfid_mapping where version='all_old') b on a.duid = b.duid;
"

