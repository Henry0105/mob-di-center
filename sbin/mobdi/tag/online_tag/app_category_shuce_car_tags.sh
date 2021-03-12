#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di

#mapping表
#app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par
#app_category_shuce_car=dm_sdk_mapping.app_category_shuce_car

#输出表
#timewindow_online_profile_v2=dm_mobdi_report.timewindow_online_profile_v2

app_pkg_mapping_partion=`hive -e "show partitions $app_pkg_mapping_par" |tail -n 1`
app_pkg_mapping_partion=${app_pkg_mapping_partion:8:13}
p90day=$(date -d "$day -90 days" +%Y%m%d)

## 生成在装线上标签
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_online_profile_v2 partition (flag=23,day=$day,timewindow='90')
select device,concat(cate,'_23_90') as feature, count(distinct apppkg) as cnt
from
(
select cleaned.device,shuce.cate,cleaned.apppkg
from
(
select device,coalesce(clean.apppkg,reserved.pkg) as apppkg
from
(
select device,pkg
from $dws_device_install_app_re_status_di
where day>'$p90day' and day<='$day'
      and refine_final_flag in (1,0,-1,2)
union all
select device,pkg
from $dws_device_install_app_status_40d_di
where day='$p90day'
      and final_flag in (0,1)
) reserved
left join
(
  select pkg,apppkg from $app_pkg_mapping_par where version='$app_pkg_mapping_partion'
)clean
on reserved.pkg=clean.pkg
) cleaned
join
(
  select apppkg,cate from $app_category_shuce_car where version='1000'
) shuce
on cleaned.apppkg=shuce.apppkg
)grouped group by device,cate
"


## 生成新安装线上标签
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_online_profile_v2 partition (flag=24,day=$day,timewindow='90')
select device,concat(cate,'_24_90') as feature, count(apppkg) as cnt
from
(
    select cleaned.device,
           shuce.cate,
           cleaned.apppkg
    from
    (
        select device,
               coalesce(clean.apppkg,reserved.pkg) as apppkg
        from
        (
            select device,pkg
            from $dws_device_install_app_re_status_di
            where day>'$p90day'
            and day<='$day'
            and refine_final_flag=1
        ) reserved
        left join
        (
            select pkg,apppkg
            from $app_pkg_mapping_par
            where version='$app_pkg_mapping_partion'
        )clean
        on reserved.pkg=clean.pkg
    ) cleaned
    join
    (
      select apppkg,
             cate
      from $app_category_shuce_car
      where version='1000'
    ) shuce
    on cleaned.apppkg = shuce.apppkg
    group by cleaned.device,shuce.cate,cleaned.apppkg
)grouped
group by device,cate;
"

## 生成卸载线上标签
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $timewindow_online_profile_v2 partition (flag=25,day=$day,timewindow='90')
select device,concat(cate,'_25_90') as feature, count(apppkg) as cnt
from
(
    select cleaned.device,shuce.cate,cleaned.apppkg
    from
    (
        select device,
               coalesce(clean.apppkg,reserved.pkg) as apppkg
        from
        (
            select device,pkg
            from $dws_device_install_app_re_status_di
            where day>'$p90day'
            and day<='$day'
            and refine_final_flag=-1
        ) reserved
        left join
        (
          select pkg,apppkg
          from $app_pkg_mapping_par
          where version='$app_pkg_mapping_partion'
        )clean
        on reserved.pkg=clean.pkg
    ) cleaned
    join
    (
        select apppkg,
               cate
        from $app_category_shuce_car
        where version='1000'
    ) shuce
    on cleaned.apppkg=shuce.apppkg
    group by cleaned.device,shuce.cate,cleaned.apppkg
)grouped group by device,cate;
"
