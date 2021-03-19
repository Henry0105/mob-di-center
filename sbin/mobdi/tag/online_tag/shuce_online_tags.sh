#!/bin/bash

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#app_active_daily=dm_mobdi_report.app_active_daily
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di

#mapping表
#app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par
#app_category_shuce=dm_sdk_mapping.app_category_shuce

#输出表
#timewindow_online_profile_v2=dm_mobdi_report.timewindow_online_profile_v2

pday=$(date -d "$day -7 days" +%Y%m%d)
p30day=$(date -d "$day -30 days" +%Y%m%d)
p90day=$(date -d "$day -90 days" +%Y%m%d)

## 生成新装app个数线上标签
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_online_profile_v2 partition (flag=18,day=$day,timewindow='30')
select device,concat(cate,'_18_30') as feature, count(distinct cleaned.apppkg) as cnt
from
(
  select device,coalesce(clean.apppkg,reserved.pkg) as apppkg,day
  from
  (
    select device,pkg,day
    from $dws_device_install_app_re_status_di
    where day>$p30day
    and day<=$day
    and refine_final_flag=1
  )reserved
  left join
  (
    select pkg,apppkg from $app_pkg_mapping_par where version='1000'
  )clean
  on reserved.pkg=clean.pkg
)cleaned
join
(
  select apppkg,cate from $app_category_shuce where version='1000'
)b
on cleaned.apppkg=b.apppkg
group by device,cate
"

## 生成在装线上标签
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_online_profile_v2 partition (flag=19,day=$day,timewindow='90')
select device,concat(cate,'_19_90') as feature, count(apppkg) as cnt
from
(
select device,cate, cleaned.apppkg
from
(
  select device,coalesce(clean.apppkg,reserved.pkg) as apppkg,day
  from
  (
    select device,pkg,day
    from $dws_device_install_app_re_status_di
    where day>$p90day
    and day<=$day
    and refine_final_flag in (1,0,-1,2)

    union all

    select device,pkg,day
    from $dws_device_install_app_status_40d_di
    where day='$p90day'
    and final_flag in (0,1)
  )reserved left join
  (
    select pkg,apppkg from $app_pkg_mapping_par where version='1000'
  )clean
  on reserved.pkg=clean.pkg
)cleaned
join
(
  select apppkg,cate from $app_category_shuce where version='1000'
)b
on cleaned.apppkg=b.apppkg
group by device,cate,cleaned.apppkg
)grouped group by device,cate
"

## 生成活跃天数标签
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_online_profile_v2 partition (flag=20,day=$day,timewindow='90')
select device,concat(cate,'_20_90') as feature, count(distinct day) as cnt
from
(
  select device,apppkg,day from $app_active_daily where day > $p90day and day <= $day
)cleaned
join
(
  select apppkg,cate from $app_category_shuce where version='1000'
)b
on cleaned.apppkg=b.apppkg
group by device,cate
"
