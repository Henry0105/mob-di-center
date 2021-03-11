#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 利用每日的增量，获取到新的标签，计算节假日的标签
@projectName:MOBDI
'
# 无model直接使用

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1

##input
device_applist_new="dm_mobdi_mapping.device_applist_new"
##mapping
mapping_special_identity="tp_mobdi_model.mapping_special_identity"

#output
label_l1_vocation_special=${label_l1_vocation_special}

hive -v -e "
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.map.aggr=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
insert overwrite table $label_l1_vocation_special partition(day=$day)
select device,max(identity) as identity
from $mapping_special_identity a1
inner join
(
  select device,pkg
  from $device_applist_new
  where day=$day
) a2 on a1.pkg = a2.pkg
group by device;
"
