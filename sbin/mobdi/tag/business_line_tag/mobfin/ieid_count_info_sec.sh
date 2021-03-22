#!/bin/sh

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

## 源表
dm_ieid_mapping_v3_sec=dm_dataengine_mapping.dm_ieid_mapping
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df

## 目标表
#label_l1_anticheat_ieid_sec_mf=dm_mobdi_report.label_l1_anticheat_ieid_sec_mf

ieid_mapping_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_dataengine_mapping', 'dm_ieid_mapping', 'day');
drop temporary function GET_LAST_PARTITION;
"
ieid_mapping_last_day=(`hive -e "$ieid_mapping_partition_sql"`)

device_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_master', 'dwd_device_info_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
device_last_version=(`hive -e "$device_partition_sql"`)

hive -v -e "
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts='-Xmx1800m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx1800m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3700m' -XX:+UseG1GC;
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.exec.reducers.bytes.per.reducer=2147483648;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.reuse.jvm.num.tasks=10;
set mapred.tasktracker.map.tasks.maximum=24;
set mapred.tasktracker.reduce.tasks.maximum=24;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;

set mapred.job.name=ieid_count_info_sec;

insert overwrite table $label_l1_anticheat_ieid_sec_mf partition(day='$day')
select
  other_info.ieid, mcid_cn, snid_cn, nvl(model_cn, 0) as model_cn, isid_cn, pid_cn
from
( select ieid,
    case when mcid is null then 0 else size(mcid) end as mcid_cn,
    case when snid is null then 0 else size(snid) end as snid_cn,
    case when isid is null then 0 else size(isid) end as isid_cn,
    case when pid is null then 0 else size(pid) end as pid_cn
  from $dm_ieid_mapping_v3_sec
  where day='$ieid_mapping_last_day'
  and plat='1'
) other_info
left join
( select
    ieid, count(1) as model_cn
  from
  ( select
      ieid, coalesce(model, '') as model, coalesce(factory, '') as factory
    from  (
            select ieid, d as device
            from $dm_ieid_mapping_v3_sec
            lateral view explode(device) devices as d
            where day='$ieid_mapping_last_day'
            and plat='1'
            and device is not null
          ) ieid_info
          left join ( select
                        device, lower(trim(factory)) as factory, trim(lower(model)) as model
                      from $dwd_device_info_df
                      where version='$device_last_version' and plat='1'
                      group by device, lower(trim(factory)), trim(lower(model))
                     ) device_info
          on ieid_info.device = device_info.device
    group by ieid, coalesce(model, ''), coalesce(factory, '')
  ) t
  where model != '' or factory != ''
  group by ieid
) model_info
on other_info.ieid = model_info.ieid
;
"
