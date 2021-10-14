#!/bin/sh

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
device_full_with_brand_mapping=dw_mobdi_tmp.device_full_with_brand_mapping
#device_all_risk_refine=dm_mobdi_report.device_all_risk_refine

## 目标表
#label_l1_anticheat_device_model_mf=dm_mobdi_report.label_l1_anticheat_device_model_mf

hive -v -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=256000000;

insert overwrite table $label_l1_anticheat_device_model_mf partition (day = '$day')
select
   t1.device,
   t1.model_risk,
   case when t2.mapping_factory is not null and t2.mapping_model is not null then '一致'
           else '未知' end as model_brand_cons
from (select device,model_risk from $device_all_risk_refine where day='$day') t1
join $device_full_with_brand_mapping t2
on t1.device=t2.device
;
"
