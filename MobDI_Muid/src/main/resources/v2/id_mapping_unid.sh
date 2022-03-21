#!/bin/bash
set -x -e

tmp_db=dm_mid_master
id_source="$tmp_db.dwd_all_id_detail"

duid_fsid_mapping=$tmp_db.duid_unid_mapping

ids_unid_vertex="$tmp_db.ids_unid_vertex"

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
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
SET hive.exec.parallel=true;
"

hive -e "
$sqlset
insert overwrite table $id_source partition(day='all')
select a.duid,oiid,ieid,factory,model,fsid unid,mid,flag
from $id_source a
left join
$duid_fsid_mapping b
on a.duid=b.duid and b.version='all'
where a.day='all'
"

hive -e "
create table if not exists $ids_unid_vertex(
id1 string,
id2 string
) partitioned by (
day string
) stored as orc;
$sqlset
with tmp_source as(
select * from $id_source where day='all'
),
tmp_ieid_unid as (
select ieid,min(unid) unid_ieid
from tmp_source
where ieid is not null and ieid<>'' group by ieid
),
tmp_oiid_unid as (
select oiid,factory,min(unid) unid_oiid
from tmp_source
where oiid is not null and oiid<>'' group by oiid,factory
)
insert overwrite table $ids_unid_vertex partition(day='all')
select id1,id2 from (
  select unid id1,unid_ieid id2
  from tmp_source a
  left join tmp_ieid_unid b on a.ieid=b.ieid
  where a.ieid is not null and a.ieid<>''

  union all

  select unid id1,unid_oiid id2
  from tmp_source a
  left join tmp_oiid_unid b on a.oiid=b.oiid and a.factory=b.factory
  where a.oiid is not null and a.oiid<>''
) t group by id1,id2
"