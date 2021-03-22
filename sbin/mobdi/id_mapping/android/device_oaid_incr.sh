#!/bin/sh
set -e -x

:<<!
@parameters
@insert_day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1

# 获取当前日期的下个月第一天
nextmonth=`date -d "${day} +1 month" +%Y%m01`
# 获取当前日期所在月的第一天
startdate=`date -d"${day}" +%Y%m01`
# 获取当前日期所在月的最后一天
enddate=`date -d "$nextmonth last day" +%Y%m%d`

# input
log_device_info_jh=dw_sdk_log.log_device_info_jh
dws_device_duid_mapping_new=dm_mobdi_topic.dws_device_duid_mapping_new
awaken_dfl=dw_sdk_log.awaken_dfl

#output
dim_device_oaid_mapping_di=dm_mobdi_mapping.dim_device_oaid_mapping_di


HADOOP_USER_NAME=dba hive -v -e "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;
set mapreduce.job.queuename=root.yarn_data_compliance2;


insert overwrite table $dim_device_oaid_mapping_di partition (day='$day',source='log_device_info_jh')
select
    trim(lower(muid)) as device,
    case
      when lower(trim(oaid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or oaid is null then ''
      when trim(oaid) rlike '^([A-Za-z0-9]|-)+$' then trim(oaid)
      else ''
    end as oaid,
    unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as oaid_tm,
    '' as duid
from $log_device_info_jh
where dt='$day'
and oaid is not null
and lower(oaid) not in ('','null')
and plat=1
and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
and trim(muid) != '0000000000000000000000000000000000000000'
;
"

:<<!
以dw_sdk_log.awaken_dfl表(oaid， duid)为基准，left join  dm_mobdi_topic.dws_device_duid_mapping_new（device，duid） ,根据duid关联，得到device，oaid
!

HADOOP_USER_NAME=dba hive -v -e "
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;
set mapreduce.job.queuename=root.yarn_data_compliance2;

insert overwrite table $dim_device_oaid_mapping_di partition (day='$day',source='awaken_dfl')
select
    b.device,
    case
      when lower(trim(a.oaid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or a.oaid is null then ''
      when trim(a.oaid) rlike '^([A-Za-z0-9]|-)+$' then trim(a.oaid)
      else ''
    end as oaid,
    a.oaid_tm,
    a.duid
from
(
  select
         get_json_object(extra, '$.cnt_fids.fids.oaid') as oaid,
         substr(serdatetime,0,10) as oaid_tm,
         lower(trim(duid)) as duid
  from $awaken_dfl
  where day='$day'
  and get_json_object(extra, '$.cnt_fids.fids.oaid') is not null
  and lower(trim(get_json_object(extra, '$.cnt_fids.fids.oaid'))) not in ('','null')
  and lower(trim(get_json_object(extra, '$.cnt_fids.fids.oaid')))  rlike '^([A-Za-z0-9]|-)+$'
  and duid is not null
  and lower(trim(duid)) not in ('','null')
) a
left join
(
  select
      device,
      duid,
      processtime
  from
  (
    select trim(lower(device)) as device,
           trim(lower(duid)) as duid,
           processtime,
           row_number() over (partition by duid order by processtime desc) as rn
    from $dws_device_duid_mapping_new
    where length(trim(device))>0
    and length(trim(duid))>0
    and trim(lower(device)) rlike '^[a-f0-9]{40}$'
    and trim(device) != '0000000000000000000000000000000000000000'
    and plat=1
  )m where rn=1
) b
on a.duid = b.duid
;

"
