#!/bin/bash

set -x -e
day=$1
preday=`date -d "$day -1 days" +%Y%m%d`
before90=`date -d "$day -90 days" +%Y%m%d`
#input
dws_device_pid_di=dm_mobdi_topic.dws_device_pid_di
dwd_ios_id_mapping_sec_di=dm_mobdi_master.dwd_ios_id_mapping_sec_di
#md
pid_muid_base_sec_di=dw_mobdi_md.pid_muid_base_sec_di
#mapping
device_profile_label_full_par=dm_mobdi_report.device_profile_label_full_par
ios_device_info_sec_df=rp_mobdi_app.ios_device_info_sec_df
#output
dim_pid_device_track_android_ios_sec_df=dim_mobdi_mapping.dim_pid_device_track_android_ios_sec_df
dim_pid_device_track_tmp=dim_mobdi_mapping.dim_pid_device_track_tmp

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=30;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;

insert overwrite table ${pid_muid_base_sec_di} partition(day='$day')
select device,pid,plat,time
from(select device,pid,plat,time,row_number() over(partition by device,pid order by time desc) as rn
     from (
           select device,pid1 as pid ,1 as plat,
           case when pid_tm1='' or pid_tm1 is null then unix_timestamp(day,'yyyyMMdd') else pid_tm1
           end as time
           from ${dws_device_pid_di}
           lateral view explode_tags(concat(pid,'=',pid_tm)) mytable as pid1,pid_tm1
           where day='$day' and pid is not null and pid <> '' and device is not null  and device <> '' and device <> '0000000000000000000000000000000000000000'
           union all
           select ifid1 as device, pid1 as pid, 2 as plat, case when pid_tm1='' or pid_tm1 is null then unix_timestamp(day,'yyyyMMdd') else pid_tm1 end as time
           from ${dwd_ios_id_mapping_sec_di}
           lateral view explode_tags(concat(pid, '=', pid_tm)) mytable as pid1, pid_tm1
           lateral view explode(split(ifid,',')) ifids as ifid1
           where day = '$day' and pid <> '' and pid is not null and device is not null and device <> '' and device <> '00000000-0000-0000-0000-000000000000'
          ) ttt
	 where pid <> '' and pid is not null
    ) tttt
where rn = 1;
"


HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=30;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;


with abnormal_flag as (
select
  pid,
  case when count(device) >= 200 then 1 else 0 end as abnormal_flag
from ${pid_muid_base_sec_di}
where day = '$day'
group by pid
),
profile_flag as (
select pid,device,time,plat,
       case when profile_flag1=1 or profile_flag2=1 then 1 else 0 end as profile_flag
from(select a.pid, a.device, a.time, a.plat,
             case when full1.device is not null and full1.first_active_time <= '$day' then 1 else 0 end as profile_flag1,
             case when full2.device is not null and full2.first_active <= '$day' then 1 else 0 end as profile_flag2
     from(
	       select pid,device,time,plat
           from ( select pid,device,time,1 as plat,row_number() over(partition by pid order by device desc) as rn
                  from ${pid_muid_base_sec_di}
				  where day = '$day'
				) t
           where rn <= 200
         ) a
	     left join
	     (select device,first_active_time
          from ${device_profile_label_full_par}
          where version='$day.1000' and applist is not null and applist <> '' and applist <> 'unknown'
         ) full1
         on a.device = full1.device
         left join
		 (select ifid as device,first_active
          from ${ios_device_info_sec_df}
          where day='$day'
         ) full2
         on a.device = full2.device
    ) tt
),
cnt as (
select
     device, pid,count(time) as cnt
from ${pid_muid_base_sec_di}
where  day <= '$day'  and day > '$before90'
group by pid,device
)

insert overwrite table ${dim_pid_device_track_tmp} partition(day='${day}')
select b.pid, abnormal_flag.abnormal_flag, device_list
from (select pid, collect_set(device_info) as device_list
      from(select profile_flag.pid,
                  named_struct('device', profile_flag.device, 'time', profile_flag.time, 'cnt', cast(cnt.cnt as int), 'profile_flag', profile_flag.profile_flag, 'plat', profile_flag.plat) as device_info
           from profile_flag
           left join cnt
           on profile_flag.pid = cnt.pid and profile_flag.device = cnt.device
           ) a
      group by pid
      ) b
left join abnormal_flag
on b.pid = abnormal_flag.pid;
"
HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx7400m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx7400m';
set mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts='-Xmx7g';
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.optimize.index.filter=true;
insert overwrite table $dim_pid_device_track_android_ios_sec_df partition(day=$day)
select pid, map_agg(device_list) as device_list,max(update_time) as update_time
from (select pid, device_list, update_time
	  from $dim_pid_device_track_android_ios_sec_df
	  where day='$preday'
      union all
      select pid, map(day,named_struct('abnormal_flag',abnormal_flag,'devices', device_list)) as device_list,day as update_time
	  from ${dim_pid_device_track_tmp}
	  where day='$day'
) t
group by pid;
"
