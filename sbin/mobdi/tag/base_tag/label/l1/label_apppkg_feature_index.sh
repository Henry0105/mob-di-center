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

if [ $# -ne 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <model>"
    exit 1
fi

day=$1
model=$2
flag=`echo $model | sed -e 's/_//g'`
version=1003_${flag}

source /home/dba/mobdi_center/conf/hive-env.sh

##input
device_applist_new=${dim_device_applist_new_di}

##mapping
#apppkg_index="tp_mobdi_model.apppkg_index"

##output
#label_l1_apppkg_feature_index="dm_mobdi_report.label_l1_apppkg_feature_index"

#得到设备的apppkg特征索引
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_l1_apppkg_feature_index partition (day = ${day},version = '$version')
select t1.device, t2.index, 1.0 as cnt
from
(
  select device, pkg
  from $device_applist_new
  where day = ${day}
) t1
inner join
$apppkg_index t2 on t1.pkg=t2.apppkg and t2.model='$model' and t2.version='1003'
group by t1.device, t2.index
"