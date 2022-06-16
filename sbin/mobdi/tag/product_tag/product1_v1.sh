#!/bin/bash

set -x -e

day=$1

echo "$day"


sourceTable1="rp_finance_anticheat_muid.ronghui_product1_v1"
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
'6156_1000',cast(rh_p1_score1 as string),
'6157_1000',cast(rh_p1_score2 as string),
'6158_1000',cast(rh_p1_score3 as string),
'6159_1000',cast(rh_p1_score4 as string),
'6160_1000',cast(rh_p1_score5 as string),
'6161_1000',cast(rh_p1_score6 as string),
'6162_1000',cast(rh_p1_score7 as string),
'6163_1000',cast(rh_p1_score8 as string),
'6164_1000',cast(rh_p1_score9 as string),
'6165_1000',cast(rh_p1_score10 as string),
'6166_1000',cast(rh_p1_score11 as string),
'6167_1000',cast(rh_p1_score12 as string),
'6168_1000',cast(rh_p1_score13 as string),
'6169_1000',cast(rh_p1_score14 as string),
'6170_1000',cast(rh_p1_score15 as string),
'6171_1000',cast(rh_p1_score16 as string),
'6172_1000',cast(rh_p1_score17 as string),
'6173_1000',cast(rh_p1_score18 as string)) as profile
from $sourceTable1
where day='$day'
and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
) union_source
group by device
cluster by device
;
"
