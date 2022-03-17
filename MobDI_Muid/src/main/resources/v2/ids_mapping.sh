#!/bin/bash
set -x -e

start_date=$1
end_date=$2

brand_mapping="dm_sdk_mapping.brand_model_mapping_par"
id_source="dm_mid_master.dwd_all_id_detail"

log_device_info_jh="dm_mobdi_master.dwd_log_device_info_jh_sec_di"
log_device_install_app_all_info="dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di"
pv="dm_mobdi_master.dwd_pv_sec_di"
location_info="dm_mobdi_master.dwd_location_info_sec_di"
auto_location_info="dm_mobdi_master.dwd_auto_location_info_sec_di"
log_wifi_info="dm_mobdi_master.dwd_log_wifi_info_sec_di"
base_station_info="dm_mobdi_master.dwd_base_station_info_sec_di"
mdata_nginx_pv="dm_mobdi_master.dwd_mdata_nginx_pv_di"


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
set mapreduce.map.java.opts=-Xmx15000m;
set mapreduce.map.memory.mb=14000;
set mapreduce.reduce.java.opts=-Xmx15000m;
set mapreduce.reduce.memory.mb=14000;
"
hive -e "
create table if not exists $id_source (
duid string,
oiid string,
ieid string,
factory string,
model string,
unid string,
mid string,
flag int
) partitioned by (
day string
) stored as orc;

$sqlset
insert overwrite table $id_source partition(day=$end_date)
select duid,oiid,ieid,
CASE
    WHEN factory IS NULL OR trim(upper(factory)) in ('','NULL','NONE','NA','OTHER','未知','UNKNOWN') THEN 'UNKNOWN'
    ELSE coalesce(upper(trim(b.clean_brand_origin)), 'OTHER')
    end AS factory,
    upper(trim(a.model)) model,
    '' unid,'' mid,0 flag
from(
  select duid,oiid,ieid,factory,model from (
    select duid,oiid,ieid,factory,model,day from $log_device_info_jh
    union all
    select duid,oiid,ieid,factory,model,day from $log_device_install_app_all_info
    union all
    select duid,oiid,ieid,factory,model,day from $pv
    union all
    select duid,oiid,ieid,factory,model,day from $location_info
    union all
    select duid,oiid,ieid,factory,model,day from $auto_location_info
    union all
    select duid,oiid,ieid,factory,model,day from $log_wifi_info
    union all
    select duid,oiid,ieid,factory,model,day from $base_station_info
    union all
    select duid,oiid,ieid,factory,model,day from $mdata_nginx_pv
  )
  where day>=$start_date and day<$end_date
  and duid is not null and trim(duid)<>'' and rlike(duid,'^(s_)?[0-9a-f]{40}|[0-9a-zA-Z\-]{36}|[0-9]{14,17}$')
  and ((oiid is not null and oiid<>'') or (ieid is not null and ieid<>''))
  group by duid,oiid,ieid,factory,model
) a
left join $brand_mapping b
 on b.version='1000'
     and upper(trim(b.brand)) = upper(trim(a.factory))
     and upper(trim(b.model)) = upper(trim(a.model))
"
