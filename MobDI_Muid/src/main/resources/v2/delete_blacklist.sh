#!/bin/bash
set -x -e

tmp_db=dm_mid_master
id_source="$tmp_db.dwd_all_id_detail"

ieid_black="$tmp_db.ieid_blacklist"
oiid_black="$tmp_db.oiid_blacklist"

ieid_blacklist="$tmp_db.ids_ieid_blacklist"
oiid_blacklist="$tmp_db.ids_oiid_blacklist"
duid_blacklist="$tmp_db.ids_duid_blacklist"


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
set mapreduce.job.queuename=root.important;
"

#一个ieid对应oiid>5

hive -e "
$sqlset
drop table if exists $ieid_blacklist;
create table $ieid_blacklist stored as orc as
select ieid from(
select ieid from (
    select ieid,oiid from $id_source
        where day='all_with_blacklist' and coalesce(ieid,'')<>'' and coalesce(oiid,'')<>''
        group by ieid,oiid
    ) b group by ieid having count(oiid)>5

union all

select ieid from (
    select ieid,duid from $id_source
    where day='all_with_blacklist' and coalesce(ieid)<>'' and coalesce(duid)<>''
    group by ieid,duid
) b group by ieid having count(duid)>200

union all

select ieid from $ieid_black
) t group by ieid
" &

#一个oiid对应ieid>3
hive -e "
$sqlset
drop table if exists $oiid_blacklist;
create table $oiid_blacklist stored as orc as
select oiid from(
select oiid from (
    select oiid,ieid from $id_source
    where day='all_with_blacklist' and coalesce(oiid,'')<>'' and coalesce(ieid,'')<>''
    group by oiid,ieid
) b group by oiid having count(ieid)>3

union all
select oiid from (
    select oiid,duid from $id_source
    where day='all_with_blacklist' and coalesce(oiid)<>'' and coalesce(duid)<>''
    group by oiid,duid
) b group by oiid having count(duid)>200

union all

select oiid from $oiid_black
) t group by oiid
" &

#一个duid,对应oiid>=3 , ieid>3

hive -e "
$sqlset
drop table if exists $duid_blacklist;
create table $duid_blacklist stored as orc as
select duid from(
select duid from (
    select duid,ieid from $id_source
        where day='all_with_blacklist' and coalesce(duid,'')<>'' and coalesce(ieid,'')<>''
        group by duid,ieid
    ) b group by duid having count(ieid)>3

union all
select duid from (

    select duid,oiid from $id_source
        where day='all_with_blacklist' and coalesce(duid,'')<>'' and coalesce(oiid,'')<>''
        group by duid,oiid
    ) b group by duid having count(oiid)>=3
) t group by duid

" &
wait


hive -e "
$sqlset
set hive.mapjoin.smalltable.filesize=500000000;
insert overwrite table $id_source partition(day='all')
select a.duid,
if(c.oiid is null,a.oiid,'') oiid,
if(d.ieid is null,a.ieid,'') ieid,
factory,model,unid,mid,flag
from $id_source a
left join $duid_blacklist b on a.duid=b.duid
left join $oiid_blacklist c on a.oiid=c.oiid
left join $ieid_blacklist d on a.ieid=d.ieid
where a.day='all_with_blacklist' and b.duid is null
and
(coalesce(if(c.oiid is null,a.oiid,''),'') <> ''
or
coalesce(if(d.ieid is null,a.ieid,''),'') <> ''
)
"
