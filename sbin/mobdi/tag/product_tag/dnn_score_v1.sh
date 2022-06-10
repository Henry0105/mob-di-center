#!/bin/bash

set -e -x

day=$1

echo "$day"

sourceTable1="rp_finance_anticheat_muid.ronguhi_dnn_score_v1"
outputTable="dm_mobdi_report.timewindow_online_profile_day_v4"


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
    '6174_1000',cast(rh_dnn_score1 as string),
    '6175_1000',cast(rh_dnn_score2 as string),
    '6176_1000',cast(rh_dnn_score3 as string),
    '6177_1000',cast(rh_dnn_score4 as string),
    '6178_1000',cast(rh_dnn_score5 as string),
    '6179_1000',cast(rh_dnn_score6 as string),
    '6180_1000',cast(rh_dnn_score7 as string),
    '6181_1000',cast(rh_dnn_score8 as string)) as profile
  from $sourceTable1
  where day='$day'
  and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
  union all
  select device,profile
  from $outputTable
  where day='$day'
)union_source
group by device
cluster by device
;
"
