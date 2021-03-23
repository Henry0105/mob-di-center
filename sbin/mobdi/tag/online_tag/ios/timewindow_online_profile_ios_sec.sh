#!/bin/bash

set -x -e

day=$1
timewindow=$2
pday=$(date -d "$day -$timewindow day" +%Y%m%d)

echo "当前日期: $day"
echo "时间窗口: $timewindow"
echo "开始日期: $pday"

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

# input
#ios_id_mapping_sec_df=dim_mobdi_mapping.ios_id_mapping_sec_df
#dws_device_sdk_run_master_di=dm_mobdi_master.dws_device_sdk_run_master_di

# mapping
#mapping_pkg_category_ios=dim_sdk_mapping.mapping_pkg_category_ios

# output
#timewindow_online_profile_ios_sec=dm_mobdi_report.timewindow_online_profile_ios_sec

ios_id_mapping_full_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'ios_id_mapping_sec_df', 'version');
"
ios_id_mapping_full_partition=(`hive -e "$ios_id_mapping_full_sql"`)

hive -v -e "
SET mapreduce.map.memory.mb=6144;
SET mapreduce.map.java.opts='-Xmx6144m';
SET mapreduce.child.map.java.opts='-Xmx6144m';
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with device_ifid_mapping_tmp as (
select device, ifid from $ios_id_mapping_sec_df
  where version='$ios_id_mapping_full_partition'
  and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0) and ifid != '' and size(split(ifid, ',')) <= 3
),

device_cate_id_mapping_tmp as (
select
	dsrm.day as day,
	dsrm.device as device,
	dsrm.pkg as pkg,
	mapping.cate_l1_id as cate_l1_id,
	IF(upper(mapping.cate_l2_id)='UNKNOWN', concat_ws('_',mapping.cate_l1_id, lower(mapping.cate_l2_id)), mapping.cate_l2_id) as cate_l2_id
from
(select day,device,pkg from $dws_device_sdk_run_master_di where day>='$pday' and day<='$day' and plat=2 group by day,device,pkg ) dsrm
join
(select apppkg,cate_l1_id,cate_l2_id from $mapping_pkg_category_ios where version='1000' and apppkg<>'' group by apppkg,cate_l1_id,cate_l2_id ) mapping
on dsrm.pkg=mapping.apppkg
),

ifid_cate_id_mapping_tmp as (
	select
		ifid_cate_tmp.ifid as ifid,
		ifid_cate_tmp.day as c_day,
		ifid_cate_tmp.pkg as pkg,
		cate_id_tmp as cate_id
	from
	(
		select
			ifid_tag as ifid,
			tm.day as day,
			tm.pkg as pkg,
			tm.cate_id as cate_id
		from
		(
			select
				dim.ifid as ifid,
				dcm.day as day,
				dcm.pkg as pkg,
				concat_ws(',',dcm.cate_l1_id, dcm.cate_l2_id) cate_id
			from device_ifid_mapping_tmp dim
			join device_cate_id_mapping_tmp dcm
			on dim.device=dcm.device
		) tm
		lateral view explode(split(ifid,',')) t as ifid_tag
	) ifid_cate_tmp
	lateral view explode(split(cate_id, ',')) p as cate_id_tmp
)

from ifid_cate_id_mapping_tmp
insert overwrite table $timewindow_online_profile_ios_sec partition(flag=2,day='$day',timewindow='$timewindow')
select ifid, cate_id,count(distinct c_day) as cnt group by ifid,cate_id
insert overwrite table $timewindow_online_profile_ios_sec partition(flag=1,day='$day',timewindow='$timewindow')
select ifid, cate_id,count(distinct pkg) as cnt group by ifid,cate_id;
"
