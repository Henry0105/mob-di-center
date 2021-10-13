#!/bin/sh
set -x -e

:<<!
计算app的在装
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#rp_device_profile_full_view=dm_mobdi_report.rp_device_profile_full_view

#mapping
#dim_p2p_app_cat=dim_sdk_mapping.dim_p2p_app_cat
#p2p_app_cat=dim_sdk_mapping.p2p_app_cat

#output
#ads_device_install_app_cash_p2p_cnt_di=dm_mobdi_report.ads_device_install_app_cash_p2p_cnt_di
#ads_device_install_app_cash_cnt_view=dm_mobdi_report.ads_device_install_app_cash_cnt_view
#ads_device_install_app_p2p_cnt_view=dm_mobdi_report.ads_device_install_app_p2p_cnt_view
#timewindow_online_profile_v3=dm_mobdi_report.timewindow_online_profile_v3

hive -v -e"
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $ads_device_install_app_cash_p2p_cnt_di partition(type,day)
select device,
       concat_ws(',',collect_set(app_mapping.pkg)) as applist,
       count(app_mapping.pkg) as cnt,
       app_mapping.cate_id as type,
       '$day' as day
from 
(
  select device,app
  from $rp_device_profile_full_view
  lateral view explode(split(applist,',')) apps as app
) profile_full
inner join
(
  select pkg ,cate_id
  from $dim_p2p_app_cat
  where cat in ('P2P借贷','现金贷')
)app_mapping
on profile_full.app=app_mapping.pkg
group by device,app_mapping.cate_id;

create or replace view $ads_device_install_app_p2p_cnt_view as
select device,applist,cnt,type,day as processtime
from $ads_device_install_app_cash_p2p_cnt_di
where type=1
and day=$day;

create or replace view $ads_device_install_app_cash_cnt_view as
select device,applist,cnt,type,day as processtime
from $ads_device_install_app_cash_p2p_cnt_di
where type=5
and day=$day;
"


#添加标签转换到v3表
hive -v -e "
SET mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx8192M';
set mapreduce.child.map.java.opts='-Xmx8192M';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts=-Xmx8192m;
set mapred.reduce.tasks=4000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table $timewindow_online_profile_v3 partition(day=${day},feature)
select device,cnt,'5353_1000' as feature
from $ads_device_install_app_cash_p2p_cnt_di
where day=${day}
and type=5

union all

select device,cnt,'5354_1000' as feature
from $ads_device_install_app_cash_p2p_cnt_di
where day=${day}
and type=1;
"
