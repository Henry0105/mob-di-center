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

isnert_day=$1

# input
dws_device_di=ex_log.dws_device_di

# output 自依赖
dim_device_merge_df=ex_log.dim_device_merge_df


last_par_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('ex_log', 'dim_device_merge_df', 'day');
drop temporary function GET_LAST_PARTITION;
"
last_par=(`hive -e "$last_par_sql"`)


HADOOP_USER_NAME=dba hive -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx6144m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144m' -XX:+UseG1GC;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;


insert overwrite table $dim_device_merge_df partition (day='$isnert_day')
select device
from (
  select device from $dws_device_di where day='$isnert_day'
  union all
  select device from $dim_device_merge_df where day='${last_par}'
) a
group by device
"

# qc 数据条数
function qc_id_mapping(){
  cd `dirname $0`
  sh /home/dba/mobdi/qc/real_time_mobdi_qc/qc_id_mapping_view_merge.sh  "$dim_device_merge_df" "${insert_day}" "$last_par" || qc_fail_flag=1
  if [[ ${qc_fail_flag} -eq 1 ]]; then
    echo 'qc失败，流程停止'
    exit 1
  fi
  echo "qc success!"
}

qc_id_mapping $insert_day



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
  hive -e "alter table $dim_device_merge_df DROP IF EXISTS PARTITION (day='${delete_day}');"
fi
# ### END DELETE