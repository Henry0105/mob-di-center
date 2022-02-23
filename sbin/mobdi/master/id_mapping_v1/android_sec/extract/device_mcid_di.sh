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
dws_device_mcid_di=dm_mobdi_topic.dws_device_mcid_di


#置空函数，简化代码
empty2null() {
f="$1"
echo "if(length($f) <= 0, null, $f)"
}


HADOOP_USER_NAME=dba hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_mcid as 'com.youzu.mob.java.udf.GetMcidByWlan0';

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5120m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5120m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx8192m' -XX:+UseG1GC;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;



insert overwrite table $dws_device_mcid_di partition(day='$insert_day')
select
    device,
    concat_ws(',', collect_list(mcid)) as mcid,
    concat_ws(',', collect_list(mcid_tm)) as mcid_tm
from
(
    select
      device,
      mcid,
      mcid_tm
    from
    (
        select
            muid as device,
            case
               when get_mcid(mcidarray) is not null and get_mcid(mcidarray) <> '02:00:00:00:00:00'
                 then lower(get_mcid(mcidarray))
               else `empty2null 'mcid'`
             end as mcid,
            case
               when get_mcid(mcidarray) is not null and get_mcid(mcidarray) <> '02:00:00:00:00:00'
                 then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
               when `empty2null 'mcid'` is not null
                 then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
               else null
             end as mcid_tm
        from $dwd_log_device_info_jh_sec_di
        where day ='$insert_day' and plat = 1 and muid is not null and length(trim(muid))=40
    ) mm
    where mcid is not null and length(mcid) > 0
) a
where mcid is not null and length(mcid) > 0
group by device
"


# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_mcid_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_mcid_di drop if exists partition ($old_version)"
done
