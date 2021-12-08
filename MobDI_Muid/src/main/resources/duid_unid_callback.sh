
#!/bin/bash
set -x -e
old_new_duid_mapping_par=dm_mid_master.old_new_duid_mapping_par
old_new_unid_mapping_par=dm_mid_master.old_new_unid_mapping_par
duid_unid_mapping=dm_mid_master.duid_unid_mapping

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
"
hive -e "
create table if not exists $old_new_duid_mapping_par(
duid string,
duid_final string
) partitioned by (version string)
stored as orc;
$sqlset
insert overwrite table $old_new_duid_mapping_par partition(version='20211031')
select b.duid duid, c.duid duid_final
from $old_new_unid_mapping_par a
left join 
(select sfid,duid from $duid_unid_mapping where version='2019-2021')b
on a.old_id = b.sfid
left join
(select sfid,duid from $duid_unid_mapping where version='2019-2021') c
on a.new_id = c.sfid
where a.month='2019-2021' and a.version='all'
;"
