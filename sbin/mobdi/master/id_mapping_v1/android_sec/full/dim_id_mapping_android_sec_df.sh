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
dim_device_merge_sec_df=dim_mobdi_mapping.dim_device_merge_sec_df
dim_device_mcid_merge_df=dim_mobdi_mapping.dim_device_mcid_merge_df
dim_device_ieid_merge_df=dim_mobdi_mapping.dim_device_ieid_merge_df
dim_device_snid_merge_df=dim_mobdi_mapping.dim_device_snid_merge_df
dim_device_isid_merge_df=dim_mobdi_mapping.dim_device_isid_merge_df
dim_device_pid_merge_df=dim_mobdi_mapping.dim_device_pid_merge_df
dim_device_oiid_merge_df=dim_mobdi_mapping.dim_device_oiid_merge_df

# output
dim_id_mapping_android_sec_df=dim_mobdi_mapping.dim_id_mapping_android_sec_df

# view
dim_id_mapping_android_sec_df_view=dim_mobdi_mapping.dim_id_mapping_android_sec_df_view



HADOOP_USER_NAME=dba hive -e"
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts='-Xmx9830m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx9830m';
set mapreduce.reduce.memory.mb=16384;
set mapreduce.reduce.java.opts='-Xmx13107m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;



insert overwrite table $dim_id_mapping_android_sec_df partition (version='${insert_day}.1000')
select
    ff.device as device,
    de_mcid.mcid as mcid,
    de_mcid.mcid_tm as mcid_tm,
    de_mcid.mcid_ltm as mcid_ltm,

    de_ieid.ieid as ieid,
    de_ieid.ieid_tm as ieid_tm,
    de_ieid.ieid_ltm as ieid_ltm,

    de_snid.snid as snid,
    de_snid.snid_tm as snid_tm,
    de_snid.snid_ltm as snid_ltm,

    de_isid.isid as isid,
    de_isid.isid_tm as isid_tm,
    de_isid.isid_ltm as isid_ltm,

    de_pid.pid as pid,
    de_pid.pid_tm as pid_tm,
    de_pid.pid_ltm as pid_ltm,

    de_oiid.oiid as oiid,
    de_oiid.oiid_tm as oiid_tm,
    de_oiid.oiid_ltm as oiid_ltm,

    de_mcid.mcid_abnormal_tm as mcid_abnormal_tm,
    de_ieid.ieid_abnormal_tm as ieid_abnormal_tm,
    de_snid.snid_abnormal_tm as snid_abnormal_tm,
    de_isid.isid_abnormal_tm as isid_abnormal_tm,
    de_pid.pid_abnormal_tm as pid_abnormal_tm,
    de_oiid.oiid_abnormal_tm as  oiid_abnormal_tm
from
(select device from $dim_device_merge_sec_df where day='${insert_day}') ff
left join
(
  select device,mcid, mcid_tm, mcid_ltm, mcid_abnormal_tm
  from $dim_device_mcid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and mcid is not null and length(mcid)>0
) de_mcid on ff.device=de_mcid.device
left join
(
  select device,ieid, ieid_tm, ieid_ltm, ieid_abnormal_tm
  from $dim_device_ieid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and ieid is not null and length(ieid)>0
) de_ieid on ff.device=de_ieid.device
left join
(
  select device, snid, snid_tm, snid_ltm, snid_abnormal_tm
  from $dim_device_snid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and snid is not null and length(snid)>0
) de_snid on ff.device=de_snid.device
left join
(
  select device, isid, isid_tm, isid_ltm,isid_abnormal_tm
  from $dim_device_isid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and isid is not null and length(isid)>0
) de_isid on ff.device=de_isid.device
left join
(
  select device, pid, pid_tm, pid_ltm,pid_abnormal_tm
  from $dim_device_pid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and pid is not null and length(pid)>0
) de_pid on ff.device=de_pid.device
left join
(
  select device, oiid, oiid_tm, oiid_ltm,oiid_abnormal_tm
  from $dim_device_oiid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and oiid is not null and length(oiid)>0
) de_oiid on ff.device=de_oiid.device
"

# qc 数据条数
function qc_id_mapping(){

  cd `dirname $0`
  sh /home/dba/mobdi/qc/real_time_mobdi_qc/qc_id_mapping_view.sh  "$dim_id_mapping_android_sec_df" "${insert_day}.1000" "$dim_id_mapping_android_sec_df_view" || qc_fail_flag=1
  if [[ ${qc_fail_flag} -eq 1 ]]; then
    echo 'qc失败，阻止生成view'
    exit 1
  fi
  echo "qc_id_mapping success!"
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
  echo "deleting version: ${delete_day}.1000 if exists"
  hive -e "alter table $dim_id_mapping_android_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1000');"
  echo "deleting version: ${delete_day}.1001 if exists"
  hive -e "alter table $dim_id_mapping_android_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1001');"
  echo "deleting version: ${delete_day}.1002 if exists"
  hive -e "alter table $dim_id_mapping_android_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1002');"
fi

# 创建/替换视图
HADOOP_USER_NAME=dba hive -e "
create or replace view $dim_id_mapping_android_sec_df_view
as
select * from $dim_id_mapping_android_sec_df
where version='${insert_day}.1000'
"
# ### END DELETE