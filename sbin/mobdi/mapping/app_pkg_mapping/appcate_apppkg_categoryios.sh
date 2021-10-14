#!/bin/bash

set -e -x

if [ $# -lt 1 ]; then
 echo "ERROR: wrong number of parameters"
 echo "USAGE: <day>"
 exit 1
fi

day=$1
pre_90_days=`date -d "day -90 days" +%Y%m%d`
formatDate=`date -d "${day}" +%Y-%m-%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dws_device_sdk_run_master_di=dm_mobdi_topic.dws_device_sdk_run_master_di

#mapping
#apppkg_cate_categoryios_mapping=dm_sdk_mapping.apppkg_cate_categoryios_mapping

#output
#ios_cate_profile_tmp=dm_mobdi_report.ios_cate_profile_tmp
#ios_cate_profile_weight_all_di=dm_mobdi_report.ios_cate_profile_weight_all_di


hive -v -e "
SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx6144M';
set mapreduce.child.map.java.opts='-Xmx6144M';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144M';
set mapreduce.child.reduce.java.opts='-Xmx6144M';

DROP TABLE IF EXISTS $ios_cate_profile_tmp;
create table $ios_cate_profile_tmp stored as orc as
select a.device,a.pkg,b.appname,b.cate_l1,b.cate_l1_id,b.cate_l2,b.cate_l2_id,a.day
from
(
    select device,
           pkg,
           day
    from $dws_device_sdk_run_master_di
    where day>='${pre_90_days}'
    and day<'${day}'
    and plat=2
    group by device,pkg,day
) a
join
(
    select apppkg,
           appname,
           cate_l1,
           cate_l1_id,
           cate_l2,
           cate_l2_id
    from $dim_apppkg_cate_categoryios_mapping
    where version='1000'
    and apppkg<>''
    group by apppkg,appname,cate_l1,cate_l1_id,cate_l2,cate_l2_id
) b
on a.pkg=b.apppkg;
"

hive -v -e "
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $ios_cate_profile_weight_all_di partition(day='${day}')
select device,
       cate_l1,
       cate_l1_id,
       cate_l2,
       cate_l2_id,
       sum(weight1) as weight
from
(
    select * ,
           case
             when datediff('${formatDate}',from_unixtime(unix_timestamp(day, 'yyyyMMdd'), 'yyyy-MM-dd'))>=30 then 0.05
             else exp(-0.1*datediff('${formatDate}',from_unixtime(unix_timestamp(day, 'yyyyMMdd'), 'yyyy-MM-dd')))
           end as weight1
    from $ios_cate_profile_tmp
) a
group by device,cate_l1,cate_l1_id,cate_l2,cate_l2_id;
"


