#!/bin/bash
set -x -e

day=$1

# input
timewindow_online_profile_day_v6_part1=dm_mobdi_report.timewindow_online_profile_day_v6_part1
timewindow_online_profile_day_v6_part2=dm_mobdi_report.timewindow_online_profile_day_v6_part2
timewindow_online_profile_day_v6_part3=dm_mobdi_report.timewindow_online_profile_day_v6_part3
timewindow_online_profile_day_v6_part4=dm_mobdi_report.timewindow_online_profile_day_v6_part4

# output
timewindow_online_profile_day_v6=dm_mobdi_report.timewindow_online_profile_day_v6


HADOOP_USER_NAME=dba hive -e "
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=500;
set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set hive.mapred.reduce.tasks.speculative.execution=false;
set mapreduce.job.queuename=root.yarn_data_compliance1;


add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
create temporary function map_concat as 'com.youzu.mob.java.map.MapConcat';
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';


insert overwrite table $timewindow_online_profile_day_v6 partition(day='${day}')
select device,map_agg(profile) as profile
from (
  select device,profile from $timewindow_online_profile_day_v6_part1 where day='${day}'
  union all
  select device,profile from $timewindow_online_profile_day_v6_part2 where day='${day}'
  union all
  select device,profile from $timewindow_online_profile_day_v6_part3 where day='${day}'
  union all
  select device,profile from $timewindow_online_profile_day_v6_part4 where day='${day}'
) un
group by device
cluster by device
"
