#!/bin/bash
set -x -e

day=$1
version="${day}_monthly_bak"

# input
timewindow_offline_profile_v2=dm_mobdi_report.timewindow_offline_profile_v2
device_profile_label_full_par=dm_mobdi_report.device_profile_label_full_par

# mapping
profile_id_mapping=dm_sdk_mapping.profile_id_mapping

# output
timewindow_online_profile_day_v6_part2=dm_mobdi_report.timewindow_online_profile_day_v6_part2


HADOOP_USER_NAME=dba hive -e "
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=18432;
set mapreduce.reduce.java.opts='-Xmx15360m' -XX:+UseG1GC;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=500;
set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set hive.mapred.reduce.tasks.speculative.execution=false;
set mapreduce.job.queuename=root.yarn_data_compliance2;


add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
create temporary function map_concat as 'com.youzu.mob.java.map.MapConcat';
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';

insert overwrite table $timewindow_online_profile_day_v6_part2 partition(day='${day}')
select trim(lower(device)) as device,map_agg(profile) as profile from
(
  select a.device,
    map(concat_ws('_',cast(b.profile_id as string),cast(b.profile_version_id as string)),
        cnt) as profile
  from
  (
  select profile_id,profile_version_id,
    regexp_replace(split(split(profile_column,'feature=')[1],' ')[0],'\'','') as feature,
    regexp_replace(split(split(profile_column,'timewindow=')[1],' ')[0],'\'','') as timewindow,
    regexp_replace(split(split(profile_column,'flag=')[1],' ')[0],'\'','') as flag
  from (
    select * from $profile_id_mapping
    where profile_version_id='1000'
    and profile_id in('4448','4449','4450',
    '4451','4452','4453','4454',
    '4455','4456','4457','4458',
    '4459','4460','4461','4462',
    '4463','4468','4470','4472',
    '4474','4476','4478','4469',
    '4471','4473','4475','4477',
    '4479','4482','4483','4490','4493')
  ) m
  where profile_table ='timewindow_offline_profile_v2'
  ) b
  join
  (select  * from $timewindow_offline_profile_v2
   where day='$day' and timewindow in ('7','30') and flag in ('1','3','6','9')
  ) a
  on a.feature=b.feature and b.flag=(cast(a.flag as string)) and a.timewindow=b.timewindow

  union all

   select device, map(
    '1_1000',gender,
    '2_1000',agebin,
    '2_1001',agebin_1001,
    '3_1000',edu,
    '4_1000',kids,
    '5_1000',income,
    '5_1001',income_1001,
    '6_1000',occupation,
    '7_1000',house,
    '8_1000',car,
    '9_1000',married,
    '10_1000',life_stage,
    '1034_1000',group_list,
    '11_1000',industry,
    '1035_1000',segment,
    '12_1000',identity,
    '1036_1000',country,
    '13_1000',special_time,
    '1037_1000',province,
    '14_1000',nationality,
    '1038_1000',city,
    '1039_1000',city_level,
    '1039_1001',city_level_1001,
    '16_1000',first_active_time,
    '17_1000',cell_factory,
    '18_1000',model,
    '20_1000',carrier,
    '21_1000',network,
    '4439_1000',permanent_country,
    '4440_1000',permanent_province,
    '4441_1000',permanent_city,
    '4465_1000',public_date,
    '4467_1000',applist,
    '5355_1000',permanent_city_level,
    '5938_1000',price,
    '5937_1000',last_active,
    '5936_1000',tot_install_apps,
    '5935_1000',breaked,
    '5934_1000',screensize,
    '5933_1000',sysver,
    '5932_1000',model_level
    ) as profile
     from $device_profile_label_full_par where version='$version'

) offline
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
