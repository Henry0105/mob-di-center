#!/bin/bash
set -x -e

tmp_db=dm_mid_master
install_all="dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di"
brand_mapping="dm_sdk_mapping.brand_model_mapping_par"

pkg_it="$tmp_db.pkg_it_duid_par_tmp"
factory_mapping="dm_mobdi_tmp.dim_muid_factory_model_category"
pkg_it_category="$tmp_db.pkg_it_duid_category_tmp"

token_vertex_par="$tmp_db.duid_vertex_par_tmp"
muid_mapping="$tmp_db.old_new_unid_mapping_par"

duid_fsid_mapping=$tmp_db.duid_unid_mapping

month=$1

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
set hive.mapjoin.smalltable.filesize=500000000;
SET hive.exec.max.created.files=5000000;
set mapreduce.job.queuename=root.yarn_data_compliance1;
"
hive -e "
$sqlset
insert overwrite table $pkg_it partition (day=$month)
select pkg_it,duid,'' serdatetime,upper(trim(factory)) factory,upper(trim(model)) model
from $pkg_it where day like '$month%' and day<>$month
group by pkg_it,duid,upper(trim(factory)),upper(trim(model))
"
hive -e "
create table if not exists $pkg_it_category (
pkg_it string,
duid string,
factory string,
model string,
category string,
unid string
)partitioned by (
month string,
version string
) stored as orc;
$sqlset
insert overwrite table $pkg_it_category partition(month,version)
select /*+ mapjoin(b) */ a.pkg_it,a.duid,
a.factory,
a.model,
case when b.category is not null then b.category else 10000 end as category,'' as unid,$month as month,
case when b.category is not null then concat(a.factory, '_' ,b.category) else 'other_10000' end as version
from
$pkg_it a left join $factory_mapping b
on a.factory = upper(trim(b.factory)) and a.model = upper(trim(b.model))
where a.day=$month
"


