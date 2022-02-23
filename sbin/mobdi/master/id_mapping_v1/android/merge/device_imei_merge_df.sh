#!/bin/sh

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
dws_device_imei_di=ex_log.dws_device_imei_di

# output  自依赖
dim_device_imei_merge_df=ex_log.dim_device_imei_merge_df


last_par_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('ex_log', 'dim_device_imei_merge_df', 'day');
drop temporary function GET_LAST_PARTITION;
"
last_par=(`hive -e "$last_par_sql"`)


HADOOP_USER_NAME=dba hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx6144m';
set mapreduce.reduce.memory.mb=14336;
set mapreduce.reduce.java.opts='-Xmx10240m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


insert overwrite table $dim_device_imei_merge_df partition (day='$insert_day')
select
  device,
  imei,
  imei_tm,
  imei_ltm
from
(
    select
        coalesce(b.device,a.device) as device,
        mobdi_array_udf('field', lower(a.imei), a.imei_tm, lower(b.imei), b.imei_tm,'min','/hiveDW/dm_mobdi_md/imei_blacklist/') as imei,
        mobdi_array_udf('date', lower(a.imei), a.imei_tm,   lower(b.imei), b.imei_tm,'min','/hiveDW/dm_mobdi_md/imei_blacklist/') as imei_tm,
        mobdi_array_udf('date', lower(a.imei), a.imei_ltm,  lower(b.imei), b.imei_tm,'max','/hiveDW/dm_mobdi_md/imei_blacklist/') as imei_ltm
    from
    (
        select *
        from $dim_device_imei_merge_df
        where day = '$last_par'
        and device is not null
        and length(device)= 40
        and device = regexp_extract(device,'([a-f0-9]{40})', 0)
    ) a
    full join
    (
        select *
        from $dws_device_imei_di
        where day='$insert_day'
        and device is not null
        and length(device)= 40
        and device = regexp_extract(device,'([a-f0-9]{40})', 0)
    ) b
    on a.device = b.device
) tt
where imei is not null and length(imei)>0
"


# qc 数据条数
function qc_id_mapping(){
  cd `dirname $0`
  sh /home/dba/mobdi/qc/real_time_mobdi_qc/qc_id_mapping_view_merge.sh  "$dim_device_imei_merge_df" "${insert_day}" "$last_par" || qc_fail_flag=1
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
  hive -e "alter table $dim_device_imei_merge_df DROP IF EXISTS PARTITION (day='${delete_day}');"
fi
# ### END DELETE