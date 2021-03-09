#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1
p1day=`date -d "$day -1 days" +%Y%m%d`
p3day=`date -d "$day -3 days" +%Y%m%d`


#input
dim_id_mapping_android_df=dm_mobdi_mapping.dim_id_mapping_android_df

#outpuT
tmp_android_id_mapping_1to1=dm_mobdi_tmp.tmp_android_id_mapping_1to1


full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_id_mapping_android_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

## 这边表在每天id_mapping跑完 再跑 更新第二天的的数据

HADOOP_USER_NAME=dba hive -v -e"
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';

insert overwrite table $tmp_android_id_mapping_1to1 partition(day='$day')
select
   device, imei
    from(
         select imei, collect_list(device) as devices
         from
         (
           select device, lower(trim(mytable.imei)) as imei
           from
           (
             select device, concat(imei, '=', imei_ltm) as imei_concat
             from $dim_id_mapping_android_df
             where version = '$full_last_version' and length(trim(imei)) > 0
           ) as a
           lateral view explode_tags(imei_concat) mytable as imei, imei_ltm
           group by device, lower(trim(mytable.imei))
         ) as b
         group by imei
        ) t1
lateral view explode(devices) mytable2 as device
where size(devices)=1;
"

## 保留最近的三个分区
HADOOP_USER_NAME=dba hive -v -e"alter table $tmp_android_id_mapping_1to1 drop partition(day='$p3day')"