#!/bin/bash

set -x -e

day=$1

echo "$day"

sourceTable1="rp_finance_anticheat_muid.ronghui_product7_v1"
outputTable="dm_mobdi_report.timewindow_online_profile_ronghui_product7"


HADOOP_USER_NAME=dba hive -v -e "
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=500;
set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set hive.mapred.reduce.tasks.speculative.execution=false;
set hive.optimize.index.filter=true;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
create temporary function map_concat as 'com.youzu.mob.java.map.MapConcat';
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';


insert overwrite table $outputTable partition (day='${day}')
select
  device,map_agg(profile) as  profile
from
(
  select trim(lower(device)) as device, map(
    '8117_1000',cast(scene as string),
    '8118_1000',cast(ci as string)) as profile
  from $sourceTable1
  where data_date='$day'
  and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
)union_source
group by device
cluster by device
;
"
