#!/bin/bash
set -x -e

mid_db="dm_mid_master"
dws_mid_duid_final_muid_mapping="$mid_db.dws_mid_duid_final_muid_mapping"
dws_mid_duid_final_muid_mapping_detail="$mid_db.dws_mid_duid_final_muid_mapping_detail"
oneton_muid="$mid_db.oneton_muid"

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

#获取全部数据,去重
hive -e "
$sqlset
insert overwrite table $dws_mid_duid_final_muid_mapping partition(day=2021)
select duid,duid_final,muid,'' muid_final,serdatetime from(
select duid,duid_final,muid,serdatetime,
row_number() over (partition by duid,duid_final,muid order by serdatetime) rn
from $dws_mid_duid_final_muid_mapping_detail
) t where rn=1
"
#前面没有匹配到duid_final的直接取duid
hive -e "
$sqlset
insert overwrite table $dws_mid_duid_final_muid_mapping partition(day)
select a.duid,coalesce(duid_final,b.duid) duid_final,muid,'',serdatetime,
case when coalesce(duid_final,b.duid) is null then 'non-duid-final' else '2021-duid-final' end as day
from $dws_mid_duid_final_muid_mapping a
left join dm_mid_master.one_2_one_duid b
on a.duid=b.duid
where a.day=2021
"
#查出一个muid对应多个duid_final的muid
hive -e "
$sqlset
drop table if exists $oneton_muid;
create table $oneton_muid stored as orc as
select muid from (
select muid,count(distinct duid_final) cnt
from $dws_mid_duid_final_muid_mapping
where day='2021-duid-final' group by muid
) t where cnt > 1 group by muid;
"

#过滤掉上一步的muid的记录
hive -e "
$sqlset
insert overwrite table $dws_mid_duid_final_muid_mapping partition(day='2021-normal')
select duid,duid_final,a.muid,muid_final,serdatetime
from $dws_mid_duid_final_muid_mapping a
left join $oneton_muid b on a.muid=b.muid
where a.day='2021-duid-final' and b.muid is null;
"
#一个duid_final对应多个muid的去最早的muid作为muid_final
hive -e "
$sqlset
insert overwrite table $dws_mid_duid_final_muid_mapping partition(day='20211101')
select duid,a.duid_final,a.muid,b.muid muid_final,serdatetime
from $dws_mid_duid_final_muid_mapping a
left join
(select * from (
select duid_final,muid,
row_number () over (partition by duid_final order by serdatetime) rn
from $dws_mid_duid_final_muid_mapping where day='2021-normal'
)t where rn=1) b
on a.duid_final = b.duid_final
where day='2021-normal';
"