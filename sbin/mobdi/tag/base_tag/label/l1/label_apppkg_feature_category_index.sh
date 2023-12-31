#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备的feature_category_index，模型计算需要这些特征
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

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

##input
device_applist_new=${dim_device_applist_new_di}

##mapping
#mapping_app_category_index_new="tp_mobdi_model.mapping_app_category_index_new"
app_category_mapping="dm_sdk_mapping.app_category_mapping_par"

##output
#apppkg_category_index="dm_mobdi_report.label_l1_apppkg_category_index"

#得到设备的app分类特征索引
hive -v -e "
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts='-Xmx4600m';
set mapreduce.child.map.java.opts='-Xmx4600m';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_l1_apppkg_category_index partition (day = ${day}, version = '1003')
select a3.device, a1.index, 1.0 as cnt
from
(
  select cate_l2, index
  from $mapping_app_category_index_new
  where version = '1003'
) as a1
inner join
(
  select apppkg, cate_l2
  from $app_category_mapping
  where version = '1000.20190621'
  group by apppkg, cate_l2
) as a2 on a1.cate_l2 = a2.cate_l2
inner join
(
  select device, pkg
  from $device_applist_new
  where day = ${day}
) a3 on a2.apppkg = a3.pkg
group by device, index;
"