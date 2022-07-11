#!/usr/bin/env bash

tb=dm_mid_master.pkg_it_duid_category_tmp_rid

queue="root.important"

start_day=20211031
end_day=20211130
month=202111

sqlset="
set mapreduce.job.queuename=$queue;


set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

set hive.exec.reducers.bytes.per.reducer=1000000000;
set hive.exec.reducers.max=5000;




add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';

"

hive -e "
$sqlset

insert overwrite table $tb partition(month='$month')

select
  b.rnid,
  pkg ,
  firstinstalltime ,
  version ,
  duid ,
  ieid ,
  oiid ,
  factory ,
  model ,
  b.unid

 from dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di  a

join

mobdi_test.rid_unid_mid_full b

on sha1(concat_ws('|',duid,ieid,oiid,factory,model))=b.rnid

where a.day>='$start_day' and a.day <='$end_day' and a.plat=1

group by
 b.rnid,
  pkg ,
  firstinstalltime ,
  version ,
  duid ,
  ieid ,
  oiid ,
  factory ,
  model ,
  b.unid
"
