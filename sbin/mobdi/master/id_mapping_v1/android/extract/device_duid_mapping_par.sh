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
log_device_info_jh=dw_sdk_log.log_device_info_jh
pv=dw_sdk_log.pv

# 改为分区表
dws_device_duid_mapping_new_par=dm_mobdi_topic.dws_device_duid_mapping_new_par


last_par_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_topic', 'dws_device_duid_mapping_new_par', 'day');
drop temporary function GET_LAST_PARTITION;
"
last_par=(`hive -e "$last_par_sql"`)



HADOOP_USER_NAME=dba hive -v -e "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=10240;
set mapreduce.map.java.opts='-Xmx8192m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx8192m';
set mapreduce.reduce.memory.mb=16384;
set mapreduce.reduce.java.opts='-Xmx12288m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


insert overwrite table $dws_device_duid_mapping_new_par partition (day='$insert_day')
select device, duid, plat, dcookie, processtime
from
(
  select device, duid, plat, dcookie, processtime,
         row_number() over (partition by device, duid, plat order by processtime asc, dcookie desc) rn
  from
  (
    select device, duid, plat, dcookie, processtime
    from $dws_device_duid_mapping_new_par
    where day='${last_par}' and  length(duid) > 0 and device is not null and length(device)>0

    union all

    select device, duid, plat, dcookie, processtime
    from
    (
      select lower(trim(muid)) as device,
             coalesce(if(lower(trim(curduid)) < 0, '', lower(trim(curduid))), lower(trim(id))) as duid,
             plat,
             '' as dcookie,
             processtime
      from $log_device_info_jh
      where dt='$insert_day'
      and plat in (1, 2) and muid is not null and length(muid)>0

      union all

      select lower(trim(muid)) as device,
             lower(trim(duid)) as duid,
             plat,
             '' as dcookie,
             day as processtime
      from $pv
      where day='$insert_day' and plat in (1, 2) and muid is not null and length(muid)>0

    ) as a
    where length(duid) > 0
    group by device, duid, plat, dcookie, processtime
  ) as b
) as c
where rn = 1
"


# full表分区清理，保留最近10天数据，同时保留每月最后一天的数据
delete_day=`date +%Y%m%d -d "${insert_day} -10 day"`
#上个月
LastMonth=`date -d "last month" +"%Y%m"`
#这个月
_todayYM=`date +"%Y%m"`
#本月第一天
CurrentMonthFirstDay=$_todayYM"01"
#本月第一天时间戳
_CurrentMonthFirstDaySeconds=`date -d "$CurrentMonthFirstDay" +%s`
#上月最后一天时间戳
_LastMonthLastDaySeconds=`expr $_CurrentMonthFirstDaySeconds - 86400`
#上个月第一天
LastMonthFistDay=`date -d @$_LastMonthLastDaySeconds "+%Y%m"`"01"
#上个月最后一天
LastMonthLastDay=`date -d @$_LastMonthLastDaySeconds "+%Y%m%d"`

if [[ "$delete_day" -ne "$LastMonthLastDay" ]]; then
  # 保留每月最后一天的数据
  # do delete thing
  echo "deleting day: ${delete_day} if exists"
  hive -e "alter table $dws_device_duid_mapping_new_par DROP IF EXISTS PARTITION (day='${delete_day}');"
fi
# ### END DELETE
