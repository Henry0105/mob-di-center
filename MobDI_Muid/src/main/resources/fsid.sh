#!/bin/bash
set -x -e

tmp_db=dm_mid_master
install_all="dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di"
brand_mapping="dm_sdk_mapping.brand_model_mapping_par"

pkg_it="$tmp_db.pkg_it_duid_par_tmp"
factory_mapping="dm_mobdi_tmp.dim_muid_factory_model_category"
pkg_it_category="$tmp_db.pkg_it_duid_category_tmp"

token_vertex_par="$tmp_db.duid_vertex_par_tmp"
muid_mapping="$tmp_db.old_new_unid_mapping_par"

duid_fsid_mapping=$tmp_db.duid_unid_mapping

sqlset="
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
"

#生成20201101-20211101的duid fsid使用的日期参数是20211106,如果要生成之前的日期的fsid,需要用小于20211106的参数,要大于udf的starttime
#此时表里只有202011-202110的数据
hive -e "

set mapreduce.map.java.opts=-Xmx20000m;
set mapreduce.map.memory.mb=19000;
set mapreduce.reduce.java.opts=-Xmx15000m;
set mapreduce.reduce.memory.mb=14000;
insert overwrite table dm_mid_master.duid_unid_mapping partition(version='2020-2021')
select duid,fsid('20211106') from (
select duid from dm_mid_master.pkg_it_duid_par_tmp
where duid is not null and trim(duid) <> '' group by duid
)t;"

#此时表里有201911-202110的数据
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
set mapreduce.map.java.opts=-Xmx20000m;
set mapreduce.map.memory.mb=19000;
set mapreduce.reduce.java.opts=-Xmx15000m;
set mapreduce.reduce.memory.mb=14000;
insert overwrite table dm_mid_master.duid_unid_mapping partition(version='2019-2021')
select coalesce(a.duid,b.duid) duid,coalesce(b.fsid,fsid('20211104')) fsid from (
select duid from dm_mid_master.pkg_it_duid_par_tmp
where day in ('201911','201912','202001','202002','202003','202004','202005','202006','202007','202008','202009','202010')
and duid is not null and trim(duid) <> '' group by duid
) a
full join
(select duid,fsid from dm_mid_master.duid_unid_mapping where version='2020-2021') b on a.duid = b.duid;
"

hive -e "
$sqlset
insert overwrite table $pkg_it_category partition(month,version)
select pkg_it,a.duid,factory,model,category,sfid unid,month,a.version
from $pkg_it_category a
left join
$duid_fsid_mapping b
on a.duid=b.duid
where b.version='2019-2021'
"