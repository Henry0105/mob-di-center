#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1
p1day=`date -d "$day -1 days" +%Y%m%d`
p3day=`date -d "$day -3 days" +%Y%m%d`

#input
android_id_mapping_sec_df=dm_mobdi_mapping.android_id_mapping_sec_df

#output
android_id_mapping_1to1_sec_di=dw_mobdi_md.android_id_mapping_1to1_sec_di


full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'android_id_mapping_sec_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

## 这边表在每天id_mapping跑完 再跑 更新第二天的的数据

hive -v -e"
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
             from $android_id_mapping_sec_df
             where version = '$full_last_version' and length(trim(ieid)) > 0
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