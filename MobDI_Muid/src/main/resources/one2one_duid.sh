#!/bin/bash
set -x -e

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
drop table if exists dm_mid_master.one_2_one_duid;
create table dm_mid_master.one_2_one_duid stored as orc as
select a.duid from
dm_mid_master.duid_unid_mapping a
left join
(select duid from dm_mid_master.old_new_duid_mapping_par where duid is not null and trim(duid)<>''
union all
select duid from dm_mid_master.blacklist_duid where cnt > 100 and duid is not null and trim(duid)<>'' group by duid
)b
on a.duid=b.duid
where a.version='2019-2021'
and b.duid is null
;
"
