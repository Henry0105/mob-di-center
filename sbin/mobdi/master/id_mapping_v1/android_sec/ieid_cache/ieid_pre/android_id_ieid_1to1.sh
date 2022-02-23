#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1
p3day=`date -d "$day -3 days" +%Y%m%d`

#input
dim_device_ieid_merge_df=dim_mobdi_mapping.dim_device_ieid_merge_df

#output
android_id_mapping_1to1_sec_di=dm_mobdi_tmp.android_id_mapping_1to1_sec_di


## 这边表在每天id_mapping跑完 再跑 更新第二天的的数据

HADOOP_USER_NAME=dba hive -v -e"
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';

insert overwrite table $android_id_mapping_1to1_sec_di partition(day='$day')
select
   device, ieid
    from(
         select ieid, collect_list(device) as devices
         from
         (
           select device, lower(trim(mytable.ieid)) as ieid
           from
           (
             select device, concat(ieid, '=', ieid_ltm) as ieid_concat
             from $dim_device_ieid_merge_df
             where day='${day}' and length(trim(ieid)) > 0
           ) as a
           lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
           group by device, lower(trim(mytable.ieid))
         ) as b
         group by ieid
        ) t1
lateral view explode(devices) mytable2 as device
where size(devices)=1;
"

## 保留最近的三个分区
HADOOP_USER_NAME=dba hive -v -e" alter table $android_id_mapping_1to1_sec_di drop partition(day='$p3day')"