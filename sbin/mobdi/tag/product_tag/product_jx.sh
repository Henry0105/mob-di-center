#!/bin/bash

set -x -e

day=$1


echo "$day"



sourceTable1="rp_finance_anticheat_muid.jiexin_xgb_classification"
outputTable="dm_mobdi_report.timewindow_online_profile_ronghui_product_jx"

HADOOP_USER_NAME=dba hive -v -e "
CREATE TABLE if not exists $outputTable (
  device string,
  profile map<string,string>)
PARTITIONED BY (
  day string COMMENT '天')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
"

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
    '8133_1000',cast(probability_1 as string)) as profile
  from $sourceTable1
  where day='$day'
  and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
)union_source
group by device
cluster by device
;
"
