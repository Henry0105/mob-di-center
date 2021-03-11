#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 利用每日的增量，获取到新的标签，最新的city_level,city_level_1001
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
#input
device_applist_new="dm_mobdi_mapping.device_applist_new"
#mapping
shuoshi_app_new="tp_mobdi_model.shuoshi_app_new"
zhuanke_app="tp_mobdi_model.zhuanke_app"
gaozhong_app_new="tp_mobdi_model.gaozhong_app_new"
#output
## label_edu_score_di="rp_mobdi_app.label_l1_edu_score_device_label"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_l1_edu_score_device_label partition(day='$day')
select device, max(tag) as label
from
(
  select apppkg as pkg, 3.0 as tag
  from $shuoshi_app_new

  union all

  select apppkg as pkg, 1.0 as tag
  from $zhuanke_app

  union all

  select apppkg as pkg, 0.0 as tag
  from $gaozhong_app_new
) as a1
inner join
(
  select device, pkg
  from $device_applist_new
  where day = '$day'
) as a2 on a1.pkg = a2.pkg
group by device;
"
