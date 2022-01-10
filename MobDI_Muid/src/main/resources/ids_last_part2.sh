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
ids_duid_final_muid_final="$mid_db.duid_final_muid_final_mapping"

device_muid_mapping_full="dm_mobdi_mapping.device_muid_mapping_full"
device_muid_mapping_par="20211202"

device_muid_mapping_full_fixed_step1="$mid_db.device_muid_mapping_full_fixed_step1"
device_muid_mapping_full_fixed="$mid_db.device_muid_mapping_full_fixed"

duid_mid_with_id="$mid_db.duid_mid_with_id"
duid_mid_without_id="$mid_db.duid_mid_without_id"

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
#3.不符合1和2的,有oiid的flag=2,无oiid有ieid的flag=3,oiid和ieid都没有的flag=4

hive -e "
$sqlset
drop table if exists $device_muid_mapping_full_fixed_step1;
create table $device_muid_mapping_full_fixed_step1 as
select device_old,device_token,
case when b.muid is not null then '' else a.muid end as muid,
token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime,muid_final,
case when b.muid is not null then 0 when muid_final is not null then 1
when oiid is not null and oiid<>'' then 2
when ieid is not null and ieid<>'' then 3
else 4 end as flag
from (
select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime
from $device_muid_mapping_full where day='$device_muid_mapping_par'
) a
left join $blacklist_muid b on a.muid = b.muid
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
  `device_old` string,
  `device_token` string,
  `muid` string,
  `token` string,
  `ieid` string,
  `mcid` string,
  `snid` string,
  `oiid` string,
  `asid` string,
  `sysver` string,
  `factory` string,
  `serdatetime` string,
  `flag` int)
  stored as orc;
$sqlset
with tmp_black_oiid as (
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

#表E=$dws_mid_ids_mapping partition(day='20211101')
#表F=$device_muid_mapping_full_fixed
#3、将表E中有设备id（ieid，oiid）的数据和表F进行合并，记为表G，按如下操作进行
#3.1、如果表E中的设备id（先oiid再ieid）能够在表F中找到，则使用表F中的muid作为mid
#3.2、如果表E中的设备id（先oiid再ieid）不能够在表F中找到，则对表E中的duid_final做sha1操作，作为mid


hive -e "
$sqlset
drop table if exists $duid_mid_with_id;
create table $duid_mid_with_id stored as orc as
select a.duid,a.oiid,a.ieid,a.duid_final,a.muid,muid_final,factory,model,serdatetime,
b.muid oiid_mid,c.muid ieid_mid,adsid,
case when b.muid is not null and b.muid <> '' or c.muid is not null and c.muid <> '' then d.muid
     else sha1(a.duid_final)
end as mid
from
(
  select duid,oiid,ieid,unid,unid_ieid,unid_oiid,unid_final,duid_final,muid,muid_final,serdatetime
  from $dws_mid_ids_mapping where day='20211101' and !((oiid is null or oiid='') and (ieid is null or ieid=''))
) a
left join
(select oiid,factory from $device_muid_mapping_full_fixed group by oiid,factory) b
on a.oiid=b.oiid and a.factory=b.factory
left join
(select ieid from $device_muid_mapping_full_fixed group by ieid) c
on a.ieid=c.ieid
left join
(select duid_final,muid from $device_muid_mapping_full_fixed group by duid_final,muid) d
on a.duid_final=d.duid_final

"

#4、将表E中没有设备id（ieid，oiid）的数据，提取duid和duid_final的关系，记为表H，并对duid_final做sha1操作，作为mid
hive -e "
$sqlset
drop table if exists $duid_mid_without_id;
create table $duid_mid_without_id stored as orc as
select duid,duid_final,sha1(duid_final) mid
from $dws_mid_ids_mapping where day='20211101' and (oiid is null or oiid='') and (ieid is null or ieid='')
group by duid,duid_final
"
