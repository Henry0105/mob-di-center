#!/bin/bash

set -e -x

day=$1
p180day=`date -d "$day -180 days" +%Y%m%d`


HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx7300m';

set hive.groupby.skewindata=true;
set hive.exec.parallel=true;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;

insert overwrite table dm_mobdi_topic.dws_device_install_status partition(day = '$day')
select device, 
       pkg, 
       install_datetime, 
       unstall_datetime, 
       all_datetime, 
       df_final_time, 
       final_flag, 
       final_time, 
       reserved_flag, 
       from_unixtime(cast(process_time/1000 as bigint),'yyyyMMdd') as process_time
from 
(
    select device, 
           pkg, 
           install_datetime, 
           unstall_datetime, 
           all_datetime, 
           df_final_time, 
           final_flag, 
           final_time, 
           reserved_flag, 
           max(final_time) over(partition by device) as process_time
    from 
    (
        select device, 
               pkg, 
               install_datetime, 
               unstall_datetime, 
               all_datetime, 
               df_final_time, 
               final_flag, 
              case 
                when df_final_time = all_datetime and df_final_time > install_datetime and df_final_time > unstall_datetime then df_final_time
                when install_datetime >= all_datetime and install_datetime >= df_final_time and install_datetime > unstall_datetime then install_datetime
                when df_final_time > all_datetime and df_final_time > install_datetime and df_final_time > unstall_datetime then df_final_time
                when unstall_datetime >= all_datetime and unstall_datetime >= df_final_time and unstall_datetime >= install_datetime then unstall_datetime
              else final_time 
              end as final_time,
              case 
                when df_final_time = all_datetime and df_final_time > install_datetime and df_final_time > unstall_datetime then 0
                when install_datetime >= all_datetime and install_datetime >= df_final_time and install_datetime > unstall_datetime then 1
                when df_final_time > all_datetime and df_final_time > install_datetime and df_final_time > unstall_datetime and install_datetime > unstall_datetime and (df_final_time - install_datetime) < 1800000 then 1 
                when df_final_time > all_datetime and df_final_time > install_datetime and df_final_time > unstall_datetime and (df_final_time - all_datetime) < 1800000 then 0 
                when df_final_time > all_datetime and df_final_time > install_datetime and df_final_time > unstall_datetime and !((install_datetime > unstall_datetime and (df_final_time - install_datetime) < 1800000) or (df_final_time - all_datetime) < 1800000) then -1 
                when unstall_datetime >= all_datetime and unstall_datetime >= df_final_time and unstall_datetime >= install_datetime then -1 
              else null 
              end as reserved_flag
        from 
        (
          select c.device, pkg, install_datetime, unstall_datetime, all_datetime, final_flag, final_time, nvl(df_final_time, '0') as df_final_time
          from 
          (
            select 
            nvl(a.device, b.device) as device,
            coalesce(a.pkg, b.pkg) as pkg,
            coalesce(b.install_datetime, '0') as install_datetime,
            coalesce(b.unstall_datetime, '0') as unstall_datetime,
            coalesce(a.all_time, '0') as all_datetime,
            case 
              when b.pkg is null then a.final_flag
              when a.pkg is null then b.final_flag
              when nvl(a.final_time, '0') > nvl(b.final_time, '0') then 0
              when nvl(a.final_time, '0') <= nvl(b.final_time, '0') then b.final_flag
            else -1
            end as final_flag,
            case 
              when b.pkg is null then a.final_time
              when a.pkg is null then b.final_time
              when nvl(a.final_time, '0') > nvl(b.final_time, '0') then nvl(a.final_time, '0')
              when nvl(a.final_time, '0') <= nvl(b.final_time, '0') then nvl(b.final_time, '0')
            else '0'
            end as final_time
            from
            (
              select device, pkg, all_time, '0' as final_flag, all_time as final_time
              from dm_mobdi_topic.dws_device_app_info_df
              where day = '$day'
            ) as a
            full join 
            (
              select m.device, pkg, install_datetime, unstall_datetime, final_flag, final_time
              from 
              (
                select device, pkg, install_datetime, unstall_datetime, final_flag, final_time
                from dm_mobdi_topic.dws_device_app_install_di
                where day = '$day'
              ) as m 
              left semi join 
              (
                select device
                from dm_mobdi_topic.dws_device_app_info_df
                where day = '$day' 
                group by device
              ) as n 
              on m.device = n.device
            ) as b 
            on a.device = b.device and a.pkg = b.pkg
          ) as c 
          left join 
          (
            select device, max(all_time) as df_final_time
            from dm_mobdi_topic.dws_device_app_info_df
            where day = '$day' 
            group by device
          ) as d 
          on c.device = d.device
        ) as e
    ) as f 
) as g; 
"
