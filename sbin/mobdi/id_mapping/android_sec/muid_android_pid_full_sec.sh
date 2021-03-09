#!/bin/bash

set -x -e

: '
@owner:hushk,luost
@describe:android_id增量表更新
@projectName:mobdi
@BusinessName:id_mapping
'

:<<!
@parameters
@insert_day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1
pday=`date +%Y%m%d -d "${insert_day} -1 day"`

#input
pid_mapping_incr=dm_mobdi_master.dwd_pid_mapping_sec_di

#output（自依赖）
pid_mapping_full_sec=dm_mobdi_mapping.pid_mapping_sec_df

:<<!
取dm_mobdi_mapping.phone_mapping_full最后一个分区的全量数据与dw_mobdi_md.phone_mapping_incr的分区数据进行全量更新
!

HADOOP_USER_NAME=dba hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';

set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5500m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5500m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5500m' -XX:+UseG1GC;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
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

insert overwrite table $pid_mapping_full_sec partition (version = '${insert_day}.1000',plat = 1)
select coalesce(a.device, b.device) as device,
       mobdi_array_udf('field', a.pidno, a.pidno_tm, b.pidno, b.pidno_tm,'min') as pidno,
       mobdi_array_udf('date', a.pidno, a.pidno_tm, b.pidno, b.pidno_tm,'min') as pidno_tm,
       mobdi_array_udf('date', a.pidno, a.pidno_ltm, b.pidno, b.pidno_tm,'max') as pidno_ltm,

       mobdi_array_udf('field', a.ext_pidno, a.ext_pidno_tm, b.ext_pidno, b.ext_pidno_tm,'min') as ext_pidno,
       mobdi_array_udf('date', a.ext_pidno, a.ext_pidno_tm, b.ext_pidno, b.ext_pidno_tm,'min') as ext_pidno_tm,
       mobdi_array_udf('date', a.ext_pidno, a.ext_pidno_ltm, b.ext_pidno, b.ext_pidno_tm,'max') as ext_pidno_ltm,

       mobdi_array_udf('field', a.sms_pidno, a.sms_pidno_tm, b.sms_pidno, b.sms_pidno_tm,'min') as sms_pidno,
       mobdi_array_udf('date', a.sms_pidno, a.sms_pidno_tm, b.sms_pidno, b.sms_pidno_tm,'min') as sms_pidno_tm,
       mobdi_array_udf('date', a.sms_pidno, a.sms_pidno_ltm, b.sms_pidno, b.sms_pidno_tm,'max') as sms_pidno_ltm,

       mobdi_array_udf('field', a.isid, a.isid_tm, b.isid, b.isid_tm,'min') as isid,
       mobdi_array_udf('date', a.isid, a.isid_tm, b.isid, b.isid_tm,'min') as isid_tm,
       mobdi_array_udf('date', a.isid, a.isid_ltm, b.isid, b.isid_tm,'max') as isid_ltm,

       mobdi_array_udf('field', a.ext_isid, a.ext_isid_tm, b.ext_isid, b.ext_isid_tm,'min') as ext_isid,
       mobdi_array_udf('date', a.ext_isid, a.ext_isid_tm, b.ext_isid, b.ext_isid_tm,'min') as ext_isid_tm,
       mobdi_array_udf('date', a.ext_isid, a.ext_isid_ltm, b.ext_isid, b.ext_isid_tm,'max') as ext_isid_ltm,

       mobdi_array_udf('field', 'a.mobauth_pid', 'a.mobauth_pid_tm', b.mobauth_pid, b.mobauth_pid_tm,'min') as mobauth_pid,
       mobdi_array_udf('date', 'a.mobauth_pid', 'a.mobauth_pid_tm', b.mobauth_pid, b.mobauth_pid_tm,'min') as mobauth_pid_tm,
       mobdi_array_udf('date', 'a.mobauth_pid', 'a.mobauth_pid_tm', b.mobauth_pid, b.mobauth_pid_tm,'max') as mobauth_pid_ltm
from
(
    select *
    from $pid_mapping_full_sec
    where version = '${pday}.1000'
    and plat = 1
    and device is not null
    and length(device)= 40
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) a
full join
(
    select *
    from $pid_mapping_incr
    where day = '$insert_day'
    and plat = 1
) b
on a.device = b.device
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
  echo "deleting version: ${delete_day}.1000 if exists"
  hive -e "alter table dm_mobdi_mapping.pid_mapping_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1000',plat=1);"
  echo "deleting version: ${delete_day}.1001 if exists"
  hive -e "alter table dm_mobdi_mapping.pid_mapping_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1001',plat=1);"
  echo "deleting version: ${delete_day}.1002 if exists"
  hive -e "alter table dm_mobdi_mapping.pid_mapping_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1002',plat=1);"
fi
# ### END DELETE