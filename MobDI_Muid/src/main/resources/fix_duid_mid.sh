#!/bin/bash
set -x -e
mid_db="dm_mid_master"
duid_mid_with_id_final="$mid_db.duid_mid_with_id_final"
duid_mid_with_id_final_fixed_step1="$mid_db.duid_mid_with_id_final_fixed_step1"
duid_fsid_mapping="$mid_db.duid_unid_mapping"
ieid_unid_tmp_final="$mid_db.ieid_unid_tmp_final"
oiid_unid_tmp_final="$mid_db.oiid_unid_tmp_final"
ids_vertex_par_final="$mid_db.ids_vertex_par_final"
ids_unid_final_mapping_final="$mid_db.ids_unid_final_mapping_final"
duid_mid_with_id_final_fixed="$mid_db.duid_mid_with_id_final_fixed"

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
set mapreduce.job.queuename=root.important;
"

hive -e "
$sqlset
drop table if exists $duid_mid_with_id_final_fixed_step1;
create table $duid_mid_with_id_final_fixed_step1 stored as orc as
select a.duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime,sfid unid,'' ieid_unid,'' oiid_unid
from $duid_mid_with_id_final a
left join
(select * from $duid_fsid_mapping where version='all') b
on if(coalesce(a.duid_final,'')='',a.duid,a.duid_final) = b.duid
"


hive -e "
$sqlset
drop table if exists $ieid_unid_tmp_final;
create table $ieid_unid_tmp_final stored as orc as
select ieid,min(unid) unid_ieid
from  $duid_mid_with_id_final_fixed_step1
where ieid is not null and ieid<>'' group by ieid
"

hive -e "
$sqlset
drop table if exists $oiid_unid_tmp_final;
create table $oiid_unid_tmp_final stored as orc as
select oiid,factory,model,min(unid) unid_oiid
from $duid_mid_with_id_final_fixed_step1
where oiid is not null and oiid<>'' group by oiid,factory,model
"

hive -e "
$sqlset
insert overwrite table $duid_mid_with_id_final_fixed_step1
select duid,oiid,a.ieid,duid_final,asid,mid,factory,model,serdatetime,unid,b.unid_ieid ieid_unid,'' oiid_unid
from $duid_mid_with_id_final_fixed_step1 a
left join $ieid_unid_tmp_final b on a.ieid = b.ieid
where a.ieid is not null and a.ieid<>''

union all

select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime,unid,ieid_unid,oiid_unid
from $duid_mid_with_id_final_fixed_step1
where ieid is null or ieid=''
"

hive -e "
$sqlset
insert overwrite table $duid_mid_with_id_final_fixed_step1
select duid,a.oiid,a.ieid,duid_final,asid,mid,a.factory,a.model,serdatetime,unid,ieid_unid,c.unid_oiid oiid_unid
from $duid_mid_with_id_final_fixed_step1 a
left join $oiid_unid_tmp_final c on a.oiid = c.oiid and a.factory = c.factory and a.model = c.model
where a.oiid is not null and a.oiid<>''

union all

select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime,unid,ieid_unid,oiid_unid
from $duid_mid_with_id_final_fixed_step1
where oiid is null or oiid=''
"

#unid 和 unid_ieid 及 unid_oiid 分别构边,跑图
hive -e "
$sqlset
create table $ids_vertex_par_final stored as orc as
select id1,id2 from(
select unid id1,ieid_unid id2 from $duid_mid_with_id_final_fixed_step1
union all
select unid id1,oiid_unid id2 from $duid_mid_with_id_final_fixed_step1
) t where id1 is not null and id1<>'' and id2 is not null and id2<>'' group by id1,id2
"
hive -e "
create table if not exists $ids_unid_final_mapping_final (
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
./muid.jar '' '' 10 $ids_vertex_par_final $ids_unid_final_mapping_final

hive -e "
$sqlset
drop table if exists $duid_mid_with_id_final_fixed;
create table $duid_mid_with_id_final_fixed stored as orc as
select duid,oiid,ieid,duid_final,asid,a.mid,factory,model,serdatetime,coalesce(c.mid,a.mid) mid_final
from $duid_mid_with_id_final_fixed_step1 a
left join $ids_unid_final_mapping_final b
on a.unid = b.old_id
left join
(
  select unid,mid from(
    select unid,mid,row_number() over (partition by unid order by serdatetime) rn
    from $duid_mid_with_id_final_fixed_step1 where coalesce(unid,'')<>'' and coalesce(mid,'')<>''
  )t where rn =1
) c
on b.new_id = c.unid
"