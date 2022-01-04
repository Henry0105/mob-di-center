#!/bin/bash
set -x -e

mid_db="dm_mid_master"
dws_mid_ids_mapping="$mid_db.dws_mid_ids_mapping"
dws_mid_duid_final_muid_mapping_detail="$mid_db.dws_mid_ids_mapping_detail"
blacklist_muid="$mid_db.blacklist_muid"
one_2_one_duid="$mid_db.one_2_one_duid"
duid_fsid_mapping="$mid_db.duid_unid_mapping"

app_unid_final_mapping="$mid_db.old_new_unid_mapping_par"

ids_vertex_par="$mid_db.duid_vertex_par_ids"
ids_unid_final_mapping="$mid_db.ids_old_new_unid_mapping_par"

all_vertex_par="$mid_db.duid_vertex_par_all"
all_unid_final_mapping="$mid_db.all_old_new_unid_mapping_par"

ieid_black="$mid_db.ieid_blacklist"

oiid_black="$mid_db.oiid_blacklist"

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
create table if not exists $dws_mid_ids_mapping(
duid string,
oiid string,
ieid string,
factory string,
model string,
unid string,
unid_ieid string,
unid_oiid string,
unid_final string,
duid_final string,
muid string,
muid_final string,
serdatetime string
) partitioned by (
day string
) stored as orc;
"

#获取全部数据,去重 88 2760 4121
hive -e "
$sqlset
insert overwrite table $dws_mid_ids_mapping partition(day=2021)
select coalesce(duid,''),
case when c.oiid is null then coalesce(a.oiid,'') else '' end as oiid,
case when b.ieid is null then coalesce(a.ieid,'') else '' end as ieid,
factory,model,
'' unid,'' unid_ieid,'' unid_oiid,'' unid_final,'' duid_final,muid,'' muid_final,serdatetime
from(
select duid,oiid,ieid,muid,factory,model,serdatetime,
row_number() over (partition by duid,oiid,ieid,muid,factory,model order by serdatetime) rn
from $dws_mid_duid_final_muid_mapping_detail
where duid is not null and duid<>''
) a
left join $ieid_black b on a.ieid=b.ieid
left join $oiid_black c on a.oiid=c.oiid
where rn=1
"

#unid缺失的重新生成(覆盖之前过滤掉的8亿duid)
hive -e "
$sqlset
add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function sfid as 'com.mob.udf.HistorySnowflakeUDF';

insert overwrite table $duid_fsid_mapping partition(version='all')
select a.duid,sfid('20211107') sfid from
(select duid from $dws_mid_ids_mapping where day='2021' group by duid) a
left join
(select duid from $duid_fsid_mapping where version='2019-2021') b
on a.duid = b.duid
where b.duid is null
union all
select duid,sfid from $duid_fsid_mapping where version='2019-2021'
"

#匹配unid --8827604121
hive -e "
$sqlset
insert overwrite table $dws_mid_ids_mapping partition(day='unid')
select a.duid,oiid,ieid,factory,model,
sfid unid,'' unid_ieid,'' unid_oiid,'' unid_final,'' duid_final,muid,'' muid_final,serdatetime
from $dws_mid_ids_mapping a
left join
(select * from $duid_fsid_mapping where version='all') b
on a.duid = b.duid
where a.day='2021'
"

#根据ieid聚合取最小的unid作为unid_ieid
#根据oiid,factory,model聚合取最小的unid作为unid_oiid
#8827604121
hive -e "
$sqlset
with ieid_tmp as(
  select ieid,min(unid) unid_ieid
  from $dws_mid_ids_mapping
  where day='unid' and ieid is not null and ieid<>'' group by ieid
),
oiid_tmp as (
  select oiid,factory,model,min(unid) unid_oiid
  from $dws_mid_ids_mapping
  where day='unid' and oiid is not null and oiid<>''
  group by oiid,factory,model
)
insert overwrite table $dws_mid_ids_mapping partition(day='unid_ieid_oiid')
select duid,a.oiid,a.ieid,factory,model,unid,
b.unid_ieid,c.unid_oiid,
'' unid_final,'' duid_final,muid,'' muid_final,serdatetime
from $dws_mid_ids_mapping a
left join ieid_tmp b on a.ieid = b.ieid
left join oiid_tmp c on a.oiid = c.oiid and a.factory = c.factory and a.model = c.model
where a.day='unid'
"

#unid 和 unid_ieid 及 unid_oiid 分别构边,跑图
hive -e "
create table $ids_vertex_par stored as orc as
select id1,id2 from(
select unid id1,unid_ieid id2 from $dws_mid_ids_mapping where day='unid_ieid_oiid'
union all
select unid id1,unid_oiid id2 from $dws_mid_ids_mapping where day='unid_ieid_oiid'
) t group by id1,id2
"
hive -e "
create table if not exists $ids_unid_final_mapping (
old_id string,
new_id string
) stored as orc;
"

/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--queue root.yarn_data_compliance1 \
--name Step2TokenConnectedComponents_ids \
--class com.mob.mid_full.Step2TokenConnectedComponents \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=300 \
--conf spark.sql.shuffle.partitions=300 \
--executor-memory 30g \
--executor-cores 2 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=256m \
--conf spark.shuffle.accurateBlockThreshold=256m \
./muid.jar '' '' 10 $ids_vertex_par $ids_unid_final_mapping


#然后加上之前的applist得到的unid-unid_final放到一个图里跑,得到最终的unid-unid_final
hive -e "
$sqlset
create table $all_vertex_par stored as orc as
select old_id id1,new_id id2 from (
select old_id,new_id from $app_unid_final_mapping where month='2019-2021'
union  all
select old_id,new_id from $ids_unid_final_mapping
) t group by id1,id2
"


hive -e "
create table if not exists $all_unid_final_mapping (
old_id string,
new_id string
) stored as orc;
"

/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--queue root.yarn_data_compliance1 \
--name Step2TokenConnectedComponents_ids \
--class com.mob.mid_full.Step2TokenConnectedComponents \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=300 \
--conf spark.sql.shuffle.partitions=300 \
--executor-memory 30g \
--executor-cores 2 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=256m \
--conf spark.shuffle.accurateBlockThreshold=256m \
./muid.jar '' '' 10 $ids_vertex_par $all_unid_final_mapping

#使用unid-unid_final匹配duid,得到duid-duid_final的mapping并匹配回dws_mid_duid_final_muid_mapping表
hive -e "
$sqlset
insert overwrite table $dws_mid_ids_mapping partition(day='unid_final')
select a.duid,oiid,ieid,factory,model,unid,unid_ieid,unid_oiid,
b.new_id unid_final,c.duid duid_final,muid,'' muid_final,serdatetime
from
(select * from $dws_mid_ids_mapping where day='unid_ieid_oiid') a
left join
$all_unid_final_mapping b on a.unid = b.old_id
left join
(select * from $duid_fsid_mapping where version='all') c on b.new_id = c.sfid
"
