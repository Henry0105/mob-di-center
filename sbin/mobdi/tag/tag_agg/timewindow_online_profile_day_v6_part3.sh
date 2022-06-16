#!/usr/bin/env bash
set -x -e

day=$1

# input
timewindow_online_profile_v2=dm_mobdi_report.timewindow_online_profile_v2

# mapping
profile_id_mapping=dm_sdk_mapping.profile_id_mapping
dim_label_reduce_reserve_profile_mapping=dim_sdk_mapping.dim_label_reduce_reserve_profile_mapping

# output
timewindow_online_profile_day_v6_part3=dm_mobdi_report.timewindow_online_profile_day_v6_part3


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

with profile_feature_mapping as (
  select profile_id,feature
from
(
select v2.profile_id,feature from
(
select concat_ws('_',cast(profile_id as string),cast(profile_version_id as string)) as profile_id,
regexp_replace(split(split(profile_column,'feature=')[1],' ')[0],'\'','') as feature
from $profile_id_mapping
) m
inner join
(
select profile_id from $dim_label_reduce_reserve_profile_mapping
where version='1000' and reserve_stat like '%保留%'
and profile_table='dm_mobdi_report.timewindow_online_profile_v2'
) v2
on m.profile_id=v2.profile_id
)tt
where feature is not null
)

insert overwrite table $timewindow_online_profile_day_v6_part3 partition(day='${day}')
select trim(lower(device)) as device,str_to_map(concat_ws(',',collect_set(profile))) as profile from
(
select a.device,concat_ws(':',b.profile_id,a.cnt) as profile
from profile_feature_mapping b
inner join
(select device,feature,cnt from $timewindow_online_profile_v2 where day='${day}') a
on a.feature=b.feature
)s
where trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
group by trim(lower(device))
cluster by trim(lower(device))
"

#只保留最近30个分区
#for old_version in `hive -e "show partitions ${outputTable} " | grep -v '_bak' | sort | head -n -30`
#do
#    echo "rm $old_version"
#    hive -v -e "alter table ${outputTable} drop if exists partition($old_version)"
#done
