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
app_category_mapping="dm_sdk_mapping.app_category_mapping_par"
#mapping_age_cate_index1="tp_mobdi_model.mapping_age_cate_index1"
#mapping_age_cate_index2="tp_mobdi_model.mapping_age_cate_index2"

##output
#apppkg_category_index="dm_mobdi_report.label_l1_apppkg_category_index"


#得到设备的app catel1分类特征索引（age模型专用）
hive -v -e "
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts='-Xmx4600m';
set mapreduce.child.map.java.opts='-Xmx4600m';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

insert overwrite table $label_l1_apppkg_category_index partition (day = '$day', version = '1003.age.cate_l1')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l1_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l1_id
      from $app_category_mapping
      where version='1000'
      group by apppkg,cate_l1_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $mapping_age_cate_index1 d on c.cate_l1_id=d.cate_l1_id
) e
group by device,index;
"

#得到设备的app catel2分类特征索引（age模型专用）

hive -v -e "
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts='-Xmx4600m';
set mapreduce.child.map.java.opts='-Xmx4600m';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

insert overwrite table $label_l1_apppkg_category_index partition (day = '$day', version = '1003.age.cate_l2')
select device,index,1.0 as cnt
from
(
  select c.device,d.index
  from
  (
    select a.device,b.cate_l2_id
    from
    (
      select device, pkg
      from $device_applist_new
      where day = '$day'
    ) a
    inner join
    (
      select apppkg,cate_l2_id
      from $app_category_mapping
      where version='1000.20200306'
      group by apppkg,cate_l2_id
    ) b on a.pkg=b.apppkg
  ) c
  inner join
  $mapping_age_cate_index2 d on c.cate_l2_id=d.cate_l2_id
) e
group by device,index;
"