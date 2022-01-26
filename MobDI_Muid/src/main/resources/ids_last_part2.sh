#!/bin/bash
set -x -e

mid_db="dm_mid_master"
dws_mid_ids_mapping="$mid_db.dws_mid_ids_mapping"

asid_black="$mid_db.asid_blacklist_full"

blacklist_muid="$mid_db.blacklist_muid"

ieid_black="$mid_db.ieid_blacklist_full"
oiid_black="$mid_db.oiid_blacklist_full"

ids_duid_final_muid_final="$mid_db.duid_final_muid_final_mapping"

device_muid_mapping_full="dm_mobdi_mapping.device_muid_mapping_full"
device_muid_mapping_par="20211202"

device_muid_mapping_full_fixed_step1="$mid_db.device_muid_mapping_full_fixed_step1"
device_muid_mapping_full_fixed="$mid_db.device_muid_mapping_full_fixed"
device_muid_mapping_full_fixed_ieid="$mid_db.device_muid_mapping_full_fixed_ieid"
device_muid_mapping_full_fixed_final="$mid_db.device_muid_mapping_full_fixed_final"
device_muid_mapping_full_fixed_final_without_blacklist="$mid_db.device_muid_mapping_full_fixed_final_without_blacklist"

duid_mid_with_id="$mid_db.duid_mid_with_id"
duid_mid_with_id_final="$mid_db.duid_mid_with_id_final"
duid_mid_with_id_final_fixed="$mid_db.duid_mid_with_id_final_fixed"
duid_mid_without_id="$mid_db.duid_mid_without_id"
duid_mid_with_id_explode="$mid_db.duid_mid_with_id_explode"
muid_with_id_unjoined_final="$mid_db.muid_with_id_unjoined_final"
duid_mid_with_id_explode_final="$mid_db.duid_mid_with_id_explode_final"
duid_mid_with_id_explode_final_fixed="$mid_db.duid_mid_with_id_explode_final_fixed"
mid_with_id="$mid_db.mid_with_id"

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

####################################part2####################################

#1.一个muid对应多个duid_final的记录置空
#一个muid对应2个以上oiid的去掉
# 一个duid_final对应多个muid的合并
# 一一对应的取muid
hive -e "
$sqlset
drop table if exists $blacklist_muid;
create table $blacklist_muid stored as orc as
select muid from (
  select muid,count(distinct duid_final) cnt
  from $dws_mid_ids_mapping
  where day='unid_final' and duid_final is not null and duid_final<>''
  group by muid having cnt > 1
) t group by muid
"

hive -e "
$sqlset
insert overwrite table $dws_mid_ids_mapping partition(day='normal')
select duid,oiid,ieid,factory,model,unid,unid_ieid,unid_oiid,unid_final,duid_final,
case when b.muid is not null then '' else a.muid end as muid,
'' muid_final,serdatetime
from $dws_mid_ids_mapping a
left join $blacklist_muid b on a.muid=b.muid
where a.day='unid_final';
"

#一个duid_final对应多个muid的取最早的muid作为muid_final
#spark才能跑过
hive -e "
$sqlset
create table $ids_duid_final_muid_final stored as orc as
select duid_final,muid,muid_final from (
    select duid_final,muid,
    first_value(muid) over (partition by duid_final order by serdatetime) muid_final
    from $dws_mid_ids_mapping
    where day='normal' and muid is not null and duid_final is not null and muid<>'' and duid_final<>''
  )t group by duid_final,muid,muid_final
"

#把muid_final根据duid_final映射会原表的20211101分区
hive -e "
$sqlset
insert overwrite table $dws_mid_ids_mapping partition(day='20211101')
select duid,oiid,ieid,factory,model,unid,unid_ieid,unid_oiid,unid_final,
duid_final,a.muid,b.muid_final muid_final,serdatetime
from $dws_mid_ids_mapping a
left join
(select muid,muid_final from $ids_duid_final_muid_final group by muid,muid_final) b
on a.muid = b.muid
where day='normal' and a.muid is not null and a.muid<>''
union all
select duid,oiid,ieid,factory,model,unid,unid_ieid,unid_oiid,unid_final,
duid_final,muid,'' muid_final,serdatetime
from $dws_mid_ids_mapping
where day='normal' and (muid is null or muid='')
"


#1.一个muid对应多个duid_final的记录置空flag=0
#2.其余的取最早的muid作为muid_final flage=1
#3.不符合1和2的,有oiid的flag=2,无oiid有ieid的flag=3
#oiid和ieid都没有的flag=4

hive -e "
$sqlset
drop table if exists $device_muid_mapping_full_fixed_step1;
create table $device_muid_mapping_full_fixed_step1 as
select device_old,device_token,
case when b.muid is not null then '' else coalesce(muid_final,a.muid) end as muid,
token,ieid,mcid,snid,oiid,
case when coalesce(b.asid)<>'' then '' else asid end asid,
sysver,factory,serdatetime,
case when b.muid is not null then 0 when muid_final is not null then 1
when oiid is not null and oiid<>'' then 2
when ieid is not null and ieid<>'' then 3
else 4 end as flag
from (
select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime
from $device_muid_mapping_full where day='$device_muid_mapping_par'
) a
left join $blacklist_muid b on a.muid = b.muid
left join $asid_black b on a.asid = b.asid
left join
(select muid,muid_final from $ids_duid_final_muid_final group by muid,muid_final) c
on a.muid=c.muid
"

#flage>=2进行下一步处理
#oiid不为空的,一个muid对应不超过2个oiid,保留muid,否则置空
#oiid为空的,如果没有ieid则保留muid,否则一个muid对应不超过3个ieid的保留muid,否则置空
hive -e "
drop table if exists $device_muid_mapping_full_fixed;
CREATE TABLE if not exists $device_muid_mapping_full_fixed (
  device_old string,
  device_token string,
  muid string,
  token string,
  ieid string,
  mcid string,
  snid string,
  oiid string,
  asid string,
  sysver string,
  factory string,
  serdatetime string,
  flag int)
  stored as orc;
$sqlset
with
tmp_black_oiid as (
  select muid from (
    select muid,oiid,factory from $device_muid_mapping_full_fixed_step1 where flag=2 group by muid,oiid,factory
  ) t group by muid having count(*) > 2
),
tmp_black_ieid as (
  select muid from (
    select muid,ieid from $device_muid_mapping_full_fixed_step1 where flag=3 group by muid,ieid
  ) t group by muid having count(ieid) > 3
),
tmp_black_muid as (
  select muid from (
    select muid from tmp_black_oiid
    union all
    select muid from tmp_black_ieid
  ) t group by muid
)
insert overwrite table $device_muid_mapping_full_fixed
select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed_step1 where flag<2 or flag=4
union all
select device_old,device_token,case when b.muid is null then a.muid else '' end as muid,
token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed_step1 a left join tmp_black_muid b on a.muid=b.muid
where flag in(2,3)
"


hive -e "
drop table if exists $device_muid_mapping_full_fixed_ieid;
CREATE TABLE if not exists $device_muid_mapping_full_fixed_ieid (
  device_old string,
  device_token string,
  muid string,
  token string,
  ieid string,
  mcid string,
  snid string,
  oiid string,
  asid string,
  sysver string,
  factory string,
  serdatetime string,
  flag int)
  stored as orc;
$sqlset
with tmp_ieid_muid as(
  select ieid,muid from(
    select ieid,muid,
    row_number() over(partition by ieid order by serdatetime) rn
    from $device_muid_mapping_full_fixed
    where coalesce(ieid,'') <> '' and coalesce(muid,'') <> '' and coalesce(serdatetime,'') <> ''
  ) t where rn = 1
)
insert overwrite table $device_muid_mapping_full_fixed_ieid
select device_old,device_token,coalesce(b.muid, a.muid) muid,
token,a.ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed a
left join tmp_ieid_muid b on a.ieid=b.ieid
where coalesce(a.ieid,'') <> ''

union all

select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed where coalesce(ieid,'') = ''
"

hive -e "
drop table if exists $device_muid_mapping_full_fixed_final;
CREATE TABLE if not exists $device_muid_mapping_full_fixed_final (
  device_old string,
  device_token string,
  muid string,
  token string,
  ieid string,
  mcid string,
  snid string,
  oiid string,
  asid string,
  sysver string,
  factory string,
  serdatetime string,
  flag int)
  stored as orc;
$sqlset
with tmp_oiid_muid as(
  select oiid,muid from(
    select oiid,muid,
    row_number() over(partition by oiid order by serdatetime) rn
    from $device_muid_mapping_full_fixed_ieid
    where coalesce(oiid,'') <> '' and coalesce(muid,'') <> '' and coalesce(serdatetime,'') <> ''
  ) t where rn = 1
)
insert overwrite table $device_muid_mapping_full_fixed_final
select device_old,device_token,coalesce(b.muid, a.muid) muid,
token,ieid,mcid,snid,a.oiid,asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed_ieid a
left join tmp_oiid_muid b on a.oiid=b.oiid
where coalesce(a.oiid,'') <> ''

union all

select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed where coalesce(oiid,'') = ''
"

hive -e "
$sqlset
drop table if exists $device_muid_mapping_full_fixed_final_without_blacklist;
create table $device_muid_mapping_full_fixed_final_without_blacklist stored as orc as
select device_old,device_token,muid,token,a.ieid,mcid,snid,a.oiid,a.asid,sysver,factory,serdatetime,flag
from $device_muid_mapping_full_fixed_final a
left join $ieid_black b on a.ieid=b.ieid
left join $oiid_black c on a.oiid=c.oiid
left join $asid_black d on a.asid=d.asid
where b.ieid is null and c.oiid is null and d.asid is null
"

hive -e "
$sqlset
insert overwrite table $dws_mid_ids_mapping partition(day='final')
select duid,a.oiid,a.ieid,factory,model,unid,unid_ieid,unid_oiid,unid_final,
duid_final,muid,muid_final,serdatetime
from $dws_mid_ids_mapping a
left join $ieid_black b on a.ieid=b.ieid
left join $oiid_black c on a.oiid=c.oiid
where a.day=20211101 and b.ieid is null and c.oiid is null
"

#表E=$dws_mid_ids_mapping partition(day='20211101')
#表F=$device_muid_mapping_full_fixed
#3、将表E中有设备id（ieid，oiid）的数据和表F进行合并，记为表G，按如下操作进行
#3.1、如果表E中的设备id（先oiid再ieid）能够在表F中找到，则使用表F中的muid作为mid
#3.2、如果表E中的设备id（先oiid再ieid）不能够在表F中找到，则对表E中的duid_final做sha1操作，作为mid


hive -e "
$sqlset
drop table if exists $duid_mid_with_id;
create table $duid_mid_with_id stored as orc as
select duid,a.oiid,ieid,duid_final,a.factory,model,serdatetime,
b.muid oiid_mid,oiid_asid
from
(
  select duid,oiid,ieid,duid_final,factory,model,
  min(case when serdatetime is null or serdatetime='' then null else serdatetime end) serdatetime
  from $dws_mid_ids_mapping
  where day='final' and oiid is not null and  oiid<>''
  group by duid,oiid,ieid,duid_final,muid,muid_final,factory,model
) a
left join
(select oiid,muid,factory,collect_set(asid) oiid_asid
from $device_muid_mapping_full_fixed_final_without_blacklist
  where coalesce(oiid,'')<>''
group by oiid,muid,factory) b
on a.oiid=b.oiid and a.factory=b.factory

union all

select duid,oiid,ieid,duid_final,factory,model,serdatetime,'' oiid_mid,array() oiid_asid
from $dws_mid_ids_mapping
where day='final' and (oiid is null or oiid='')
"

hive -e "
$sqlset
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';
drop table if exists $duid_mid_with_id_final;
create table $duid_mid_with_id_final stored as orc as
select duid,oiid,a.ieid,duid_final,
case when oiid_asid is null then ieid_asid
when ieid_asid is null then oiid_asid
else array_distinct(split(concat_ws(',',oiid_asid,ieid_asid),',')) end asid,
case when oiid_mid is not null and oiid_mid <> '' then oiid_mid
      when (b.muid is not null and b.muid <> '') then b.muid
     else sha1(a.duid_final)
end as mid,
factory,model,serdatetime
from $duid_mid_with_id a
left join
(
  select ieid,muid,collect_set(asid) ieid_asid
  from $device_muid_mapping_full_fixed_final_without_blacklist
  where coalesce(ieid,'')<>''
  group by ieid,muid
) b on a.ieid=b.ieid
where a.ieid is not null and a.ieid<>''

union all

select duid,oiid,ieid,duid_final,oiid_asid asid,
case when (oiid_mid is not null and oiid_mid <> '') then oiid_mid else sha1(duid_final) end as mid
,factory,model,serdatetime
from $duid_mid_with_id
where ieid is null or ieid=''
"
#使用图合并这一步的结果
sh fix_duid_mid.sh

hive -e "
$sqlset
drop table if exists $duid_mid_with_id_explode;
create table $duid_mid_with_id_explode stored as orc as
select duid,oiid,ieid,duid_final,asid_tmp asid,mid_final mid,
factory,model,serdatetime from $duid_mid_with_id_final_fixed
LATERAL VIEW explode(coalesce(asid,array())) tmpTable as asid_tmp
"

hive -e "
$sqlset
drop table if exists $muid_with_id_unjoined_final;
create table $muid_with_id_unjoined_final like $duid_mid_with_id_explode;
with without_ieid as (
  select '' duid,oiid,a.ieid,'' duid_final,asid,muid mid,factory,null model,serdatetime
  from $device_muid_mapping_full_fixed_final_without_blacklist a
  left join
  (select ieid from $dws_mid_ids_mapping where day='final' group by ieid) b
  on a.ieid = b.ieid
  where coalesce(a.ieid,'') <>'' and b.ieid is null

  union all

  select '' duid,oiid,ieid,'' duid_final,asid,muid mid,factory,null model,serdatetime
  from $device_muid_mapping_full_fixed_final_without_blacklist
  where coalesce(ieid,'') =''
),
without_oiid as (
  select duid,a.oiid,ieid,duid_final,asid,mid,factory,model,serdatetime
  from without_ieid a
  left join
  (select oiid from $dws_mid_ids_mapping where day='final' group by oiid) b
  on a.oiid=b.oiid
  where coalesce(a.oiid,'') <>'' and b.oiid is null

  union all

  select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime
  from without_ieid where coalesce(oiid,'') =''
)
insert overwrite table $muid_with_id_unjoined_final
select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime from without_oiid
"

sh fix_unjoined_mid.sh

#展开后的数据+修复后的全量映射表去掉图合并结果join到的记录+由于asid为空展开失败的数据
hive -e "
$sqlset
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';
drop table if exists $duid_mid_with_id_explode_final;
create table $duid_mid_with_id_explode_final stored as orc as
select * from (
  select duid,oiid,ieid,duid_final,asid,factory,
  case when coalesce(mid,'')<>'' then mid
       when coalesce(duid_final,'')<>'' then sha1(duid_final)
       when coalesce(duid,'')<>'' then sha1(duid)
       else '' end as mid,
  min(if(serdatetime ='',null,serdatetime)) serdatetime
  from(
    select duid,oiid,ieid,duid_final,asid,mid,factory,serdatetime from $duid_mid_with_id_explode
    union all
    select duid,oiid,ieid,duid_final,asid,mid_final mid,factory,serdatetime from $muid_with_id_unjoined_unid_fixed
    union all
    select duid,oiid,ieid,duid_final,'' asid,mid,factory,serdatetime from $duid_mid_with_id_final_fixed
    where asid is null or size(asid)=0
  )a where coalesce(oiid,'') <>'' or coalesce(ieid,'') <>'' or coalesce(duid,'') <>''
  group by duid,oiid,ieid,duid_final,asid,mid,factory
)b where mid<>''
"
##根据duid聚合取最老的一条mid作为最终mid
##mid为空的,是从图计算到后续计算都没有关联到的,算是一对一的数据,直接取duid为duid_final
#hive -e "
#$sqlset
#add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
#create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';
#
#drop table if exists $duid_mid_with_id_explode_final_fixed;
#
#create table $duid_mid_with_id_explode_final_fixed like $duid_mid_with_id_explode;
#with duid_mid as (
#  select duid,mid_final from
#  (
#    select duid,first_value(mid) over (partition by duid order by serdatetime) mid_final
#    from $duid_mid_with_id_explode_final where coalesce(duid,'')<>'' and coalesce(mid,'')<>''
#  ) t group by duid,mid_final
#)
#insert overwrite table $duid_mid_with_id_explode_final_fixed
#select duid,oiid,ieid,duid_final,asid,if(coalesce(mid,'')='',sha1(duid),mid) mid,factory,model,
#min(if(serdatetime ='',null,serdatetime)) serdatetime from(
#  select a.duid,oiid,ieid,duid_final,asid,mid_final mid,factory,model,serdatetime
#  from $duid_mid_with_id_explode_final a
#  left join duid_mid b on a.duid=b.duid
#  where coalesce(a.duid,'')<>''
#
#  union all
#
#  select duid,oiid,ieid,duid_final,asid,mid,factory,model,serdatetime
#  from $duid_mid_with_id_explode_final
#  where coalesce(duid,'')=''
#) t where coalesce(ieid,'')<>'' or coalesce(oiid,'')<>''
#group by duid,oiid,ieid,duid_final,asid,mid,factory,model
#"
#
#hive -e "
#$sqlset
#drop table if exists $mid_with_id;
#create table $mid_with_id stored as orc as
#select duid,oiid,ieid,duid_final,asid,mid,factory,
#min(if(serdatetime ='',null,serdatetime)) serdatetime
#from $duid_mid_with_id_explode_final_fixed
#group by duid,oiid,ieid,duid_final,asid,factory,mid
#"

#4、将表E中没有设备id（ieid，oiid）的数据，提取duid和duid_final的关系，记为表H，并对duid_final做sha1操作，作为mid
#如果duid有的行有ieid或oiid并且有mid,那么这个duid不应该出现在该结果表
hive -e "
$sqlset
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';
drop table if exists $duid_mid_without_id;
create table $duid_mid_without_id stored as orc as
select a.duid,duid_final,if(coalesce(duid_final,'')='',sha1(duid),sha1(duid_final)) mid
from $dws_mid_ids_mapping a
left join
(select duid from $duid_mid_with_id_explode_final where coalesce(mid,'')<>'' and coalesce(duid,'')<>'' group by duid) b
on a.duid=b.duid
where day='final' and coalesce(a.duid,'')<>'' and coalesce(a.ieid,'')='' and coalesce(a.oiid,'')='' and b.duid is null
group by a.duid,duid_final
"