#!/usr/bin/env bash


queue="root.important"
tmpSourceTbl=dm_mid_master.pkg_it_duid_category_tmp_rid
rnid_ieid_blacklist=dm_mid_master.rnid_ieid_blacklist

# rnid 黑名单
# 单个rnid，同一个app一个版本安装10次以上


sqlset="

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;

set mapreduce.job.queuename=$queue;
set hive.merge.size.per.task = 128000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';

"



hive -e "
$sqlset

insert overwrite table $rnid_ieid_blacklist partition(type='month_install10')

select rnid from
( select rnid,pkg,version,count(distinct firstinstalltime) cnt
  from  $tmpSourceTbl
  where firstinstalltime not like '%000'
  group by rnid,pkg,version
  ) a where cnt > 10
;

insert overwrite table $rnid_ieid_blacklist partition(type='month_rnid600')
select rnid
from
( select rnid,pkg,version,firstinstalltime
 from $tmpSourceTbl
        where firstinstalltime not like '%000'
        group by rnid,pkg,version,firstinstalltime
        )tmp
    group by rnid
having count(1) > 600;

insert overwrite table $rnid_ieid_blacklist partition(type='month_pkg400')
select rnid from (
select rnid, count(distinct pkg ) pkg_cnt
   from  $tmpSourceTbl
    where  firstinstalltime not like '%000'
  group by rnid
) c where  pkg_cnt>400

;
"