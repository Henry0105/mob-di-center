#!/bin/sh

set -e -x

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1

# input
dws_device_ieid_di=dm_mobdi_topic.dws_device_ieid_di
dim_device_ieid_merge_df=dim_mobdi_mapping.dim_device_ieid_merge_df

# output (自依赖)
dim_device_id_abnormal_sec_df=dim_mobdi_mapping.dim_device_id_abnormal_sec_df


full_par=`hive -e "show partitions $dim_device_id_abnormal_sec_df" | grep "flag=ieid" |sort -rn |awk -F "/" '{print $1}'| awk -F "=" '{print $2}' | head -n 1`
echo "$full_par"


HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6553m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx6553m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6553m' -XX:+UseG1GC;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=536870912;
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

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';

insert overwrite table $dim_device_id_abnormal_sec_df partition(day='${insert_day}',flag='ieid')
select coalesce(c.device,d.device) as device,
       coalesce(c.ieid,d.ieid) as id,
       case
         when c.ieid_tm is null then d.ieid_tm
         when d.ieid_tm is null then c.ieid_tm
         when c.ieid_tm <= d.ieid_tm then c.ieid_tm
       else d.ieid_tm end as id_tm,
       case
         when c.ieid_ltm is null then d.ieid_ltm
         when d.ieid_ltm is null then c.ieid_ltm
         when c.ieid_ltm <= d.ieid_ltm then d.ieid_ltm
       else c.ieid_ltm end as id_ltm
from
(

  select device,ieid,min(ieid_tm) as ieid_tm,max(ieid_tm) as ieid_ltm
  from
  (
    select device,col1 as ieid,col2 as ieid_tm
    from
    (
        select x.device,concat_ws('=',ieid,ieid_tm) as list
        from
        (
            select device,ieid,ieid_tm
            from $dws_device_ieid_di
            where day = '${insert_day}'
        )x
        left semi join
        (
            select device
            from $dim_device_ieid_merge_df
            where day = '${insert_day}'
            and ieid_abnormal_tm <> 'unknown'
        )y
        on x.device = y.device
    )a
    lateral view explode_tags(list) mytable as col1, col2
  )x
  group by device,ieid

)c
full join
(
  select device,id as ieid,id_tm as ieid_tm,id_ltm as ieid_ltm
  from $dim_device_id_abnormal_sec_df
  where day='${full_par}' and flag='ieid'
)d
on c.device = d.device and c.ieid = d.ieid;
"

# 表分区清理，保留最近10天数据，同时保留每月最后一天的数据
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
  echo "deleting version: ${delete_day}.1000 if exists"
  hive -e "alter table $dim_device_id_abnormal_sec_df DROP IF EXISTS PARTITION (day='${delete_day}',flag='ieid');"
fi
# ### END DELETE
