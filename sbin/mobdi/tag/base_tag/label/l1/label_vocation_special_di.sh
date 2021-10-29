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

source /home/dba/mobdi_center/conf/hive-env.sh

date=$1

##input
device_applist_new=${dim_device_applist_new_di}
##mapping
#mapping_special_identity=tp_mobdi_model.mapping_special_identity

#output
label_l1_vocation_special=${label_l1_vocation_special}

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
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

INSERT OVERWRITE TABLE $label_l1_vocation_special PARTITION (day=$day)
SELECT device
     , MAX(identity) AS identity
FROM
(
  SELECT device
       , CASE
           WHEN identity = '团购商家' THEN 100
           WHEN identity = '酒店商家' THEN 101
           WHEN identity = '滴滴快的司机' THEN 102
           WHEN identity = '代驾司机' THEN 103
           WHEN identity = '货车司机' THEN 104
           WHEN identity = '专车司机' THEN 105
           WHEN identity = '快递员' THEN 106
           WHEN identity = '外卖配送员' THEN 107
           WHEN identity = '外卖商家' THEN 108
           WHEN identity = '京东商家' THEN 109
           WHEN identity = '淘宝商家' THEN 110
           WHEN identity = '微店商家' THEN 111
           WHEN identity = '医疗从业者' THEN 112
           WHEN identity = '会计从业者' THEN 113
           WHEN identity = '幼师' THEN 114
           WHEN identity = '互联网从业者' THEN 115
         ELSE -1
         END AS identity
  from $mapping_special_identity AS a1
  INNER JOIN
  (
    SELECT device
         , pkg
    FROM $device_applist_new
    WHERE day='$day'
  ) AS a2 ON a1.pkg = a2.pkg
) AS a3
GROUP BY device;
"
