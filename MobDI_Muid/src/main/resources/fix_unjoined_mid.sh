#!/bin/bash
set -x -e
mid_db="dm_mid_master"

muid_with_id_unjoined_final="$mid_db.muid_with_id_unjoined_final"
id_unjoined_ieid_unid="$mid_db.id_unjoined_ieid_unid"
id_unjoined_oiid_unid="$mid_db.id_unjoined_oiid_unid"
muid_with_id_unjoined_unid="$mid_db.muid_with_id_unjoined_unid"
muid_with_id_unjoined_vertex="$mid_db.muid_with_id_unjoined_vertex"
muid_with_id_unjoined_unid_final="$mid_db.muid_with_id_unjoined_unid_final"
muid_with_id_unjoined_unid_fixed="$mid_db.muid_with_id_unjoined_unid_fixed"


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
add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function sfid as 'com.mob.udf.HistorySnowflakeUDF';
drop table if exists $id_unjoined_ieid_unid;
create table $id_unjoined_ieid_unid stored as orc as
select ieid,sfid('20220125') unid from (
  select ieid
  from $muid_with_id_unjoined_final where coalesce(ieid,'') <> ''
  group by ieid
) t
"

hive -e "
$sqlset
drop table if exists $muid_with_id_unjoined_unid;
create table $muid_with_id_unjoined_unid stored as orc as
select duid,oiid,a.ieid,duid_final,asid,mid,factory,model,serdatetime,unid from (
  select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime
  from $muid_with_id_unjoined_final where coalesce(ieid,'') <> ''
) a left join $id_unjoined_ieid_unid b on a.ieid=b.ieid
union all
select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime,null unid
from $muid_with_id_unjoined_final where coalesce(ieid,'') = '' and coalesce(oiid,'') <> ''
"

hive -e "
$sqlset
drop table if exists $id_unjoined_oiid_unid;
create table $id_unjoined_oiid_unid stored as orc as
select oiid,factory,sfid('20220126') unid from (
  select oiid,factory
  from $muid_with_id_unjoined_unid where unid is null and coalesce(oiid,'') <> ''
  group by oiid,factory
) t
"

hive -e "
$sqlset
insert overwrite table $muid_with_id_unjoined_unid stored as orc as
select duid,a.oiid,ieid,duid_final,asid,mid,a.factory,model,serdatetime,b.unid unid from (
  select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime
  from $muid_with_id_unjoined_unid where unid is null
) a left join $id_unjoined_oiid_unid b on a.oiid=b.oiid and a.factory=b.factory
union all
select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime,unid
from $muid_with_id_unjoined_unid where unid is not null
"

hive -e "
create table if not exists $muid_with_id_unjoined_vertex(
id1 string,
id2 string
) stored as orc;

$sqlset
insert overwrite table $muid_with_id_unjoined_vertex
select id1,id2 from (
  select unid id1,min(unid) over (partition by ieid) id2 from (
    select unid,ieid from $muid_with_id_unjoined_unid where coalesce(ieid,'')<>'' group by unid,ieid
  )
  union all
  select unid id1,min(unid) over (partition by oiid,factory) id2 from (
    select unid,oiid,factory from $muid_with_id_unjoined_unid where coalesce(oiid,'')<>'' group by unid,oiid,factory
  )
)t where coalesce(id1,'')<>'' and coalesce(id2,'')<>''
group by id1,id2
"

hive -e "
create table if not exists $muid_with_id_unjoined_unid_final (
old_id string,
new_id string
) stored as orc;
"

/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--queue root.important \
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
./muid.jar '' '' 10 $muid_with_id_unjoined_vertex $muid_with_id_unjoined_unid_final


hive -e "
$sqlset
drop table if exists $muid_with_id_unjoined_unid_fixed;

CREATE TABLE $muid_with_id_unjoined_unid_fixed(
  `duid` string,
  `oiid` string,
  `ieid` string,
  `duid_final` string,
  `asid` string,
  `mid` string,
  `factory` string,
  `model` string,
  `serdatetime` string,
  `mid_final` string
)
stored as orc;
with old_id_mid as (
select old_id,mid from $muid_with_id_unjoined_unid_final b
left join
(
  select unid,mid from(
    select unid,mid,row_number() over (partition by unid order by serdatetime) rn
    from $muid_with_id_unjoined_unid where coalesce(unid,'')<>'' and coalesce(mid,'')<>''
  )t where rn =1
) c
on b.new_id = c.unid
)

insert overwrite table $muid_with_id_unjoined_unid_fixed
select duid,oiid,ieid,duid_final,asid,a.mid,factory,model,serdatetime,coalesce(b.mid,a.mid) mid_final
from $muid_with_id_unjoined_unid a
left join old_id_mid b
on a.unid = b.old_id
"
