#!/bin/bash
set -e -x

tmp_db=dm_mid_master

install_all="dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di"
brand_mapping="dm_sdk_mapping.brand_model_mapping_par"

pkg_it="$tmp_db.pkg_it_duid_par_tmp"
factory_mapping="dm_mobdi_tmp.dim_muid_factory_model_category"
pkg_it_category="$tmp_db.pkg_it_duid_category_tmp"

token_vertex_par="$tmp_db.duid_vertex_par_tmp"
muid_mapping="$tmp_db.old_new_duid_mapping_par_tmp"

start_date=$1
end_date=$2


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
set mapreduce.job.queuename=root.yarn_data_compliance1;
"

hive -e "
$sqlset
create table if not exists $pkg_it(
pkg_it string,
duid string,
serdatetime string,
factory string,
model string
) partitioned by (
day string
) stored as orc;

$sqlset
insert overwrite table $pkg_it partition (day)
select
    pkg_it,duid,serdatetime,
    CASE
        WHEN lower(trim(factory)) in ('null','none','na','other')
          OR factory IS NULL OR trim(upper(factory)) in ('','未知','UNKNOWN') THEN 'unknown'
        ELSE coalesce(upper(trim(brand_mapping.clean_brand_origin)), 'other')
        end AS factory,
    tmp.model model,day
from
    (
        select concat(pkg,'_',version,'_',firstinstalltime) pkg_it,duid,serdatetime,factory,model,day
        from $install_all
        where day>='$start_date' and day<='$end_date'
          and duid is not null and firstinstalltime is not null and length(firstinstalltime)==13
    ) tmp
left join $brand_mapping brand_mapping
   on brand_mapping.version='1000'
       and upper(trim(brand_mapping.brand)) = upper(trim(tmp.factory))
       and upper(trim(brand_mapping.model)) = upper(trim(tmp.model))
"


