#!/bin/bash

set -e -x

:<<!
CREATE TABLE dm_mobdi_topic.dws_device_app_install_di(
  device string COMMENT '设备号', 
  pkg string COMMENT '安装包', 
  install_flag int COMMENT '安装标识 1标识安装 0标识没有安装',
  unstall_flag int COMMENT '卸载标识 1标识卸载 0表示没有卸载',
  install_datetime string COMMENT '最终安装时间', 
  unstall_datetime string COMMENT '最终卸载时间', 
  final_flag int COMMENT '最终状态 -1 卸载 1 安装', 
  final_time string COMMENT '最终状态所对应的时间')
PARTITIONED BY (day string COMMENT '日期')
stored as orc;
!

day=$1
p180day=`date -d "$day -180 days" +%Y%m%d`

#1.生成近半年的di表
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';

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

insert overwrite table dm_mobdi_topic.dws_device_app_install_di partition(day='$day')
select device,
       pkg,
       max(install_flag) as install_flag,
       max(unstall_flag) as unstall_flag,
       max(install_datetime) as install_datetime,
       max(unstall_datetime) as unstall_datetime,
       case when max(install_datetime)>max(unstall_datetime) then 1 else -1 end as final_flag,
       case when max(install_datetime)>max(unstall_datetime) then max(install_datetime) else max(unstall_datetime) end as final_time
from
(
  select trim(lower(muid)) as device,
         trim(pkg) as pkg,
         0 as install_flag,
         1 as unstall_flag,
         0 as install_datetime,
         if(length(cast(clienttime as string))=13 and clienttime > 0,
           clienttime,
           concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')) as unstall_datetime,
         concat_ws('=',
           if(length(cast(clienttime as string))=13 and clienttime > 0,
             clienttime,
             concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')),
           '-1') as trace
  from dm_mobdi_master.dwd_log_device_unstall_app_info_sec_di
  where day >= '$p180day'
  and day <= '$day'
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
  and pkg is not null
  and trim(pkg) not in ('','null','NULL')
  and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\.\_-]+)',0)
  and (((serdatetime is not null and trim(serdatetime) not in ('','-1'))) or clienttime rlike '^1\\\d{12}$')

  union all

  select trim(lower(muid)) as device,
         trim(pkg) as pkg,
         1 as install_flag,
         0 as unstall_flag,
         if(length(cast(clienttime as string))=13 and clienttime > 0,
           clienttime,
           concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')) as install_datetime,
         0 as unstall_datetime,
         concat_ws('=',
           if(length(cast(clienttime as string))=13 and clienttime > 0,
             clienttime,
             concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')),
           '1') as trace
  from dm_mobdi_master.dwd_log_device_install_app_incr_info_sec_di
  where day >= '$p180day'
  and day <= '$day'
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
  and pkg is not null
  and trim(pkg) not in ('','null','NULL')
  and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\.\_-]+)',0)
  and ((serdatetime is not null and trim(serdatetime) not in ('','-1')) or clienttime rlike '^1\\\d{12}$')
)a
group by device,pkg;
"
