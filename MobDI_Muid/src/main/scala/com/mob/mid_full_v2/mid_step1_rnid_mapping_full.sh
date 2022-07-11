#!/usr/bin/env bash


start_day=20211001
end_day=20211130

queue="root.important"

#step1 抽取11张表数据 sha1 5个组合字段，union all 后全局去重复后，编号生成unid

#dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
#dm_mobdi_master.dwd_log_device_install_app_incr_info_sec_di
#dm_mobdi_master.dwd_log_device_unstall_app_info_sec_di

#dm_mobdi_master.dwd_pv_sec_di
#dm_mobdi_master.dwd_log_device_info_jh_sec_di
#dm_mobdi_master.dwd_mdata_nginx_pv_di

#dm_mobdi_master.dwd_location_info_sec_di
#dm_mobdi_master.dwd_auto_location_info_sec_di
#dm_mobdi_master.dwd_log_wifi_info_sec_di
#dm_mobdi_master.dwd_base_station_info_sec_di
#dm_mobdi_master.dwd_t_location_sec_di

#applist、applist_install、applist_unstall、 一个rid对应的pkgit数量小于7个，该rid对应的整条数据删除 pv、mdata、jh、 location_info、auto_location_info、log_wifi_info、base_station_info、t_location

#生成2月的全部组合id，rnid，映射，mid暂为空
#create table mobdi_test.rid_unid_mid_full(rid_tmp String,rnid String,unid String,mid String) partitioned by(day string) stored as orc;



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

set mapreduce.job.queuename=$queue;
set hive.merge.size.per.task = 128000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';

"


hive -e "
$sqlset

INSERT OVERWRITE TABLE mobdi_test.rid_unid_mid_full partition(day='202110-11')
select rid_tmp, sha1(rid_tmp) rnid ,fsid('20211001'),null  from
(
select rid_tmp from (

select concat_ws('|',coalesce(curduid,id),ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_log_device_info_jh_sec_di where day>='$start_day' and day <=$end_day and plat='1'
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_pv_sec_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_mdata_nginx_pv_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_location_info_sec_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_auto_location_info_sec_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_log_wifi_info_sec_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_base_station_info_sec_di where day>='$start_day' and day <='$end_day' and plat='1'
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_t_location_sec_di where day>='$start_day' and day <='$end_day' and plat='1'
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_log_device_unstall_app_info_sec_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_log_device_install_app_incr_info_sec_di where day>='$start_day' and day <='$end_day' and plat=1
union all
select concat_ws('|',duid,ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di where day>='$start_day' and day <='$end_day' and plat=1
)tmp1 group by rid_tmp
)tmp2

;"
