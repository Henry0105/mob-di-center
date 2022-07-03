#!/usr/bin/env bash


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

queue="root.important"

start_day=20211001
end_day=2021130
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

select concat_ws('|',coalesce(curduid,id),ieid,oiid,factory,model) rid_tmp from dm_mobdi_master.dwd_log_device_info_jh_sec_di where day>='20211001' and day <=$end_day and plat='1'
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




#set mapreduce.map.memory.mb=8192;
#set mapreduce.map.java.opts='-Xmx7300m';
#set mapreduce.child.map.java.opts='-Xmx8192m';
#set mapreduce.reduce.memory.mb=7300;
#set mapreduce.reduce.java.opts='-Xmx7300m';


####-------------脚本2
#!/usr/bin/env bash

tb=dm_mid_master.pkg_it_duid_category_tmp_rid

queue="root.important"

start_day=20211001
end_day=20211030
sqlset="
set mapreduce.job.queuename=$queue;


set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

set hive.exec.reducers.bytes.per.reducer=1000000000;
set hive.exec.reducers.max=5000;




add jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar;
create temporary function fsid as 'com.mob.udf.HistorySnowflakeUDF';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager.jar;
create temporary function sha1 as 'com.youzu.mob.java.udf.SHA1Hashing';

"

hive -e "
$sqlset

insert overwrite table $tb partition(month='202110')

select
  b.rnid,
  pkg ,
  firstinstalltime ,
  version ,
  duid ,
  ieid ,
  oiid ,
  factory ,
  model ,
  b.unid

 from dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di  a

join

mobdi_test.rid_unid_mid_full b

on sha1(concat_ws('|',duid,ieid,oiid,factory,model))=b.rnid

where a.day>='$start_day' and a.day <='$end_day' and a.plat=1

group by
 b.rnid,
  pkg ,
  firstinstalltime ,
  version ,
  duid ,
  ieid ,
  oiid ,
  factory ,
  model ,
  b.unid
"


## 脚本3
tmpSourceTbl=dm_mid_master.pkg_it_duid_category_tmp_rid
rnid_ieid_blacklist=dm_mid_master.rnid_ieid_blacklist
# rnid 黑名单
# 单个rnid，同一个app一个版本安装10次以上

hive -e "
$sqlset

insert overwrite table $rnid_ieid_blacklist partition(type='month_install10')

select rnid from
( select rnid,version,count(distinct firstinstalltime) cnt
  from  $tmpSourceTbl
  where firstinstalltime not like '%000'
  group by rnid,version
  ) a where cnt > 10
;

insert overwrite table $rnid_ieid_blacklist partition(type='month_rnid600')
select rnid
from
( select rnid,pkg,version,firstinstalltime
 from $tmpSourceTbl
        where firstinstalltime not like '%000'
        group by rnid,pkg,version,firstinstalltime
        )tmp
    group by rnid
having count(1) > 600;

insert overwrite table $rnid_ieid_blacklist partition(type='month_pkg400')
select rnid from (
select rnid, count(distinct pkg ) pkg_cnt
   from  $tmpSourceTbl
    where  firstinstalltime not like '%000'
  group by rnid
) c where  pkg_cnt>400

;
"
