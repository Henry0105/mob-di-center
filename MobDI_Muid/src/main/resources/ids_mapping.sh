#!/bin/bash
set -x -e

start_date=$1
end_date=$2

tmp_db=dm_mid_master
install_all="$tmp_db.dwd_log_device_install_app_all_info_sec_di"
brand_mapping="dm_sdk_mapping.brand_model_mapping_par"
duid_final_muid_mapping="$tmp_db.dws_mid_ids_mapping_detail"

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
oiid string,
ieid string,
muid string,
serdatetime string,
factory string,
model string
) partitioned by (
day string
) stored as orc;

$sqlset
insert overwrite table $duid_final_muid_mapping partition(day=$end_date)
select duid,oiid,ieid,muid,serdatetime,
CASE
    WHEN factory IS NULL OR trim(upper(factory)) in ('','NULL','NONE','NA','OTHER','未知','UNKNOWN') THEN 'unknown'
    ELSE coalesce(upper(trim(b.clean_brand_origin)), 'other')
    end AS factory,
    a.model model
from(
select duid,oiid,ieid,muid,serdatetime,factory,model
from $install_all where day>=$start_date and day<$end_date
and ((duid is not null and trim(duid)<>'')
     or (oiid is not null and oiid<>'')
     or (ieid is not null and ieid<>''))
group by duid,oiid,ieid,muid,serdatetime,factory,model
) a
left join $brand_mapping b
   on b.version='1000'
       and upper(trim(b.brand)) = upper(trim(a.factory))
       and upper(trim(b.model)) = upper(trim(a.model))

"
