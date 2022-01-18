#!/bin/bash
set -x -e

mid_db="dm_mid_master"
device_muid_mapping_full_fixed_final="$mid_db.device_muid_mapping_full_fixed_final"
ieid_black="$mid_db.ieid_blacklist"
oiid_black="$mid_db.oiid_blacklist"
ieid_blacklist="$mid_db.ieid_blacklist_full"
asid_blacklist="$mid_db.asid_blacklist_full"
oiid_blacklist="$mid_db.oiid_blacklist_full"
dws_mid_ids_mapping="$mid_db.dws_mid_ids_mapping"

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
#黑名单规则：
#一个imei对应10个以上adsid或5个以上oaid
hive -e "
$sqlset
drop table if exists $ieid_blacklist;
create table $ieid_blacklist stored as orc as
select ieid from(
    select ieid from (
        select ieid,asid from $device_muid_mapping_full_fixed_final
        where coalesce(ieid)<>'' and coalesce(asid)<>''
    group by ieid,asid
) a group by ieid having count(asid)>10

union all

select ieid from (
    select ieid,oiid from $device_muid_mapping_full_fixed_final
        where coalesce(ieid)<>'' and coalesce(oiid)<>''
        group by ieid,oiid
    ) b group by ieid having count(oiid)>5

union all

select ieid from $ieid_black

union all

select ieid from (
    select ieid,duid from $dws_mid_ids_mapping
        where day='20211101' and coalesce(ieid)<>'' and coalesce(duid)<>''
        group by ieid,duid
    ) b group by ieid having count(duid)>200
) t group by ieid

" &
#一个oaid对应10个以上adsid或3个以上imei

hive -e "
$sqlset
drop table if exists $oiid_blacklist;
create table $oiid_blacklist stored as orc as
select oiid from(
    select oiid from (
        select oiid,asid from $device_muid_mapping_full_fixed_final
        where coalesce(oiid)<>'' and coalesce(asid)<>''
    group by oiid,asid
) a group by oiid having count(asid)>10

union all

select oiid from (
    select oiid,ieid from $device_muid_mapping_full_fixed_final
        where coalesce(oiid)<>'' and coalesce(ieid)<>''
        group by oiid,ieid
    ) b group by oiid having count(ieid)>5

union all

select oiid from $oiid_black

union all

select oiid from (
    select oiid,duid from $dws_mid_ids_mapping
        where day='20211101' and coalesce(oiid)<>'' and coalesce(duid)<>''
        group by oiid,duid
    ) b group by oiid having count(duid)>200
) t group by oiid

" &
#一个adsid对应3个以上imei或5个以上oaid
hive -e "
$sqlset
drop table if exists $asid_blacklist;
create table $asid_blacklist stored as orc as
select asid from(
    select asid from (
        select asid,ieid from $device_muid_mapping_full_fixed_final
        where coalesce(asid)<>'' and coalesce(ieid)<>''
    group by asid,ieid
) a group by asid having count(ieid)>10

union all

select asid from (
    select asid,oiid from $device_muid_mapping_full_fixed_final
        where coalesce(asid)<>'' and coalesce(oiid)<>''
        group by asid,oiid
    ) b group by asid having count(oiid)>5
" &

wait

