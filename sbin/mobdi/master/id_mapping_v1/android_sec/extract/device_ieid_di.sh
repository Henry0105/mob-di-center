#!/bin/bash

set -e -x

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

insert_day=$1

# input
dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

# output
dws_device_ieid_di=dm_mobdi_topic.dws_device_ieid_di


HADOOP_USER_NAME=dba hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function ieid_array_union as 'com.youzu.mob.java.udf.IeidArrayUnion';

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx4608m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx4608m';
set mapreduce.reduce.memory.mb=14336;
set mapreduce.reduce.java.opts='-Xmx11468m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;


insert overwrite table $dws_device_ieid_di partition(day='$insert_day')
select
  device,
  concat_ws(',', collect_list(ieid)) as ieid,
  concat_ws(',', collect_list(ieid_tm)) as ieid_tm
from
(
      select
          device,
          concat_ws(',', if(size(collect_list(ieid))>0,collect_list(ieid),null),if(size(collect_list(ieid_arr))>0,collect_list(ieid_arr),null)) as ieid,
          concat_ws(',', if(size(collect_list(ieid_tm))>0,collect_list(ieid_tm),null),if(size(collect_list(ieid_arr_tm))>0,collect_list(ieid_arr_tm),null)) as ieid_tm
      from
      (
            select
                muid as device,
                if(length(trim(ieid)) = 0 or ieid is null,null,trim(ieid)) as ieid,

                case
                   when length(ieid_array_union('field',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                     then ieid_array_union('field',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                   else null
                 end as ieid_arr,

                cast(if(length(trim(ieid)) = 0 or ieid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as ieid_tm,

                case
                 when length(ieid_array_union('field',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                   then ieid_array_union('date',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                 else null
                end as ieid_arr_tm
            from $dwd_log_device_info_jh_sec_di
            where day ='$insert_day' and plat = 1 and muid is not null and length(trim(muid))=40 and (ieid is not null or ieidarray is not null)
      ) a group by device
) tt
where ieid is not null and length(trim(ieid))>0
group by device
"

# 分区清理，保留最近14个分区
for old_version in `hive -e "show partitions $dws_device_ieid_di" | grep -v '_bak' | sort | head -n -14`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_ieid_di drop if exists partition ($old_version)"
done
