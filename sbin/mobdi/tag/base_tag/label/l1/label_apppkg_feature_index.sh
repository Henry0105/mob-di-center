#!/bin/bash
set -x -e
: '
@owner:liuyanqiang
@describe: 计算设备的feature_index，模型计算需要这些特征
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1

##input
device_applist_new=${dim_device_applist_new_di}
##mapping
apppkg_index="tp_mobdi_model.apppkg_index"
##output
label_apppkg_feature_index=${label_l1_apppkg_feature_index}

#得到设备的apppkg特征索引
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_apppkg_feature_index partition (day = ${day},version = '1003_common')
select t1.device, t2.index, 1.0 as cnt
from
(
  select device, pkg
  from $device_applist_new
  where day = ${day}
) t1
inner join
$apppkg_index t2 on t1.pkg=t2.apppkg and t2.model='common' and t2.version='1003'
group by t1.device, t2.index
"

#得到设备的apppkg特征索引（income模型专用）
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_apppkg_feature_index partition (day = ${day},version = '1003_income')
select t1.device, t2.index, 1.0 as cnt
from
(
  select device, pkg
  from $device_applist_new
  where day = ${day}
) t1
inner join
$apppkg_index t2 on t1.pkg=t2.apppkg and t2.model='income' and t2.version='1003'
group by t1.device, t2.index
"

#得到设备的apppkg特征索引（income_1001模型专用）
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_apppkg_feature_index partition (day = '$day',version = '1003_income1001')
select a.device,b.index,1.0 as cnt
from
(
  select device,pkg
  from $device_applist_new
  where day = '$day'
) a
inner join
$apppkg_index b on a.pkg=b.apppkg and b.model='income_1001' and b.version='1003';
"

#得到设备的apppkg特征索引（occupation_1001模型专用）
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_apppkg_feature_index partition (day = '$day',version = '1003_occupation1001')
select a.device,b.index,1.0 as cnt
from
(
  select device,pkg
  from $device_applist_new
  where day = '$day'
) a
inner join
$apppkg_index b on a.pkg=b.apppkg and b.model='occupation_1001' and b.version='1003';
"

#得到设备的apppkg特征索引（gender模型专用）
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_apppkg_feature_index partition (day = '$day',version = '1003_gender')
select a.device,b.index,1.0 as cnt
from
(
  select device,pkg
  from $device_applist_new
  where day = '$day'
) a
inner join
$apppkg_index b on a.pkg=b.apppkg and b.model='gender' and b.version='1003';
"
