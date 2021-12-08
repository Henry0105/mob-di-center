#!/bin/bash
set -x -e

start_date=$1
end_date=$2

tmp_db=dm_mid_master
install_all="dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di"

old_new_duid_mapping_par="dm_mid_master.old_new_duid_mapping_par"

duid_final_muid_mapping="dm_mid_master.dws_mid_duid_final_muid_mapping"

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
set mapreduce.map.java.opts=-Xmx25000m;
set mapreduce.map.memory.mb=24000;
set mapreduce.reduce.java.opts=-Xmx25000m;
set mapreduce.reduce.memory.mb=24000;
"
hive -e "
create table if not exists $duid_final_muid_mapping (
duid string,
duid_final string,
muid string,
muid_final string,
serdatetime string
) partitioned by (
day string
) stored as orc;

$sqlset
insert overwrite table $duid_final_muid_mapping partition(day=$end_date)
select a.duid,duid_final,muid,'' muid_final,serdatetime from (
select duid,muid,serdatetime from(
select duid,muid,serdatetime,row_number() over(partition by duid,muid order by serdatetime) rn
from $install_all where day>=$start_date and day<$end_date
and duid is not null and trim(duid)<>''
group by duid,muid,serdatetime
) t where rn = 1) a
left join $old_new_duid_mapping_par b on a.duid=b.duid
where b.version='20211031' 
"
