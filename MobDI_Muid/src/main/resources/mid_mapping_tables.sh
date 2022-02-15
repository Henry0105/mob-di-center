#!/bin/bash
set -x -e

day=$1

mid_db="dm_mid_master"
duid_mid_with_id_explode_final="$mid_db.duid_mid_with_id_explode_final"
duid_mid_without_id="$mid_db.duid_mid_without_id"

duid_mid_mapping_par="$mid_db.duid_mid_mapping_par"
oiid_mid_mapping_par="$mid_db.oiid_mid_mapping_par"
ieid_mid_mapping_par="$mid_db.ieid_mid_mapping_par"

queue="root.yarn_etl.etl"

sqlset="
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapreduce.job.queuename=$queue;
set hive.merge.size.per.task = 128000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
"

hive -e "
$sqlset
insert overwrite table $duid_mid_mapping_par partition(day=$day)
select duid,mid from(
select duid,mid from $duid_mid_with_id_explode_final
union all
select duid,mid from $duid_mid_without_id
)t where coalesce(duid,'')<>''
group by duid,mid
"

hive -e "
$sqlset
insert overwrite table $oiid_mid_mapping_par partition(day=$day)
select oiid,factory,mid from $duid_mid_with_id_explode_final
where coalesce(oiid,'')<>''
group by oiid,factory,mid
"

hive -e "
$sqlset
insert overwrite table $ieid_mid_mapping_par partition(day=$day)
select ieid,mid from $duid_mid_with_id_explode_final
where coalesce(ieid,'')<>''
group by ieid,mid
"
