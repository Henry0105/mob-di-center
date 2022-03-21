#!/bin/bash
set -x -e

day=$1

mid_db="dm_mid_master"
dwd_all_id_detail="$mid_db.dwd_all_id_detail"

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
insert overwrite table $duid_mid_mapping_par partition(day='$day')
select duid,mid from $dwd_all_id_detail where day='all' group by duid,mid
"

hive -e "
$sqlset
insert overwrite table $oiid_mid_mapping_par partition(day='$day')
select oiid,factory,mid from(
  select oiid,factory,mid,count(mid) over(partition by oiid,factory) cnt
  from (
    select oiid,factory,mid from $dwd_all_id_detail
    where day='all' and coalesce(oiid,'')<>'' group by oiid,factory,mid
  )a
)b where cnt=1
"

hive -e "
$sqlset
insert overwrite table $ieid_mid_mapping_par partition(day='$day')
select ieid,mid from(
  select ieid,mid,count(mid) over(partition by ieid) cnt
  from (
    select ieid,mid from $dwd_all_id_detail
    where day='all' and coalesce(ieid,'')<>'' group by ieid,mid
  ) a
)b where cnt=1
"
