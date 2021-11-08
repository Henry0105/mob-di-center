#!/bin/bash

set -x -e

: '
@owner:luost
@describe:device用户标签画像：catelist对应置信度
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

#输入参数接收
day=$1

#database
tmp_database=dm_mobdi_tmp

#input
device_cate_preference_incr=${tmp_database}.device_cate_preference_incr

#output
device_cate_preference_list=${tmp_database}.device_cate_preference_list

:<<!
CREATE TABLE dw_mobdi_md.device_cate_preference_list(
  device string COMMENT '设备ID',
  cate_preference_list string COMMENT 'app分类偏好度列表'
)
PARTITIONED BY (day string COMMENT '日期')
STORED AS ORC;
!

hive -v -e "
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

set mapreduce.map.memory.mb=15360;
set mapreduce.map.java.opts='-Xmx13800m';
set mapreduce.child.map.java.opts='-Xmx13800m';

INSERT OVERWRITE TABLE $device_cate_preference_list PARTITION(day = '$day')
SELECT device,
       CONCAT(CONCAT_WS(',',COLLECT_LIST(cate_id)),'=',CONCAT_WS(',',COLLECT_LIST(CAST(preference AS string)))) AS cate_preference_list
FROM $device_cate_preference_incr
WHERE day = '${day}'
GROUP BY device;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${device_cate_preference_list} " | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${device_cate_preference_list} drop if exists partition($old_version)"
done