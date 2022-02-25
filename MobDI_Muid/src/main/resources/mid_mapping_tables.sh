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
)t where coalesce(duid,'')<>'' and rlike(duid,'^(s_)?[0-9a-f]{40}|[0-9a-zA-Z\-]{36}|[0-9]{14,17}$')
and rlike(mid,'^[0-9a-z]{40}|[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$')
group by duid,mid
"

hive -e "
$sqlset
insert overwrite table $oiid_mid_mapping_par partition(day=$day)
select oiid,factory,mid from(
  select oiid,factory,mid,count(mid) over(partition by oiid,factory) cnt
  from(
    select oiid,factory,mid
    from $duid_mid_with_id_explode_final
    where coalesce(oiid,'')<>'' and rlike(mid,'^[0-9a-z]{40}|[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$')
    group by oiid,factory,mid
  )a
)b where cnt=1
"

hive -e "
$sqlset
insert overwrite table $ieid_mid_mapping_par partition(day=$day)
select ieid,mid from $duid_mid_with_id_explode_final
where coalesce(ieid,'')<>'' and rlike(mid,'^[0-9a-z]{40}|[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$')
group by ieid,mid
"
