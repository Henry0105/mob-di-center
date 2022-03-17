#!/bin/bash
set -x -e

day=$1

tmp_db=dm_mid_master

id_source="$tmp_db.dwd_all_id_detail"

duid_fsid_mapping=$tmp_db.duid_unid_mapping

all_graph_result="$tmp_db.all_graph_result"


hive -e "
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
SET hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';
with old_id_duid_final as (
select old_id,b.duid duid_final from $all_graph_result a
left join $duid_fsid_mapping b on a.new_id = b.sfid and b.version='all'
)
insert overwrite table $id_source partition(day='$day')
select t.duid,t.oiid,t.ieid,t.factory,t.model,t.unid,sha1(duid_final) mid,flag
from $id_source t
left join old_id_duid_final a on t.unid=a.old_id
where t.day='$day'
"

