#!/bin/bash
set -e -x

day=$1
pday=`date -d "$day -1 day" +%Y%m%d`
p180day=`date -d "$day -180 days" +%Y%m%d`

#2.生成近半年的df表
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.exec.parallel=true;
set hive.groupby.skewindata=true;
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

insert overwrite table dm_mobdi_topic.dws_device_app_info_df partition(day='$day')
select device,pkg,max(all_time)
from
(
  select device,pkg,all_time
  from dm_mobdi_topic.dws_device_app_info_df
  where day = '$pday'
  and from_unixtime(cast(all_time/1000 as bigint),'yyyyMMdd') >= '$p180day'

  union all

  select device,pkg,
         if(length(cast(clienttime as string))=13 and clienttime > 0,
           clienttime,
           concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')) as all_time
  from
  (
    select trim(lower(muid)) as device,
           trim(pkg) as pkg,
           serdatetime,
           clienttime
    from dm_mobdi_master.dwd_log_device_install_app_all_info_sec_di
    where day = '$day'
    and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
    and pkg is not null
    and trim(pkg) not in ('','null','NULL')
    and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\.\_-]+)',0)
    and ((serdatetime is not null and trim(serdatetime) not in ('','-1')) or clienttime rlike '^1\\d{12}$')
  ) aa
)b
group by device,pkg
;"
