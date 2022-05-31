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
blacklist=dm_sdk_mapping.blacklist

# output
dws_device_imei_di=ex_log.dws_device_imei_di


HADOOP_USER_NAME=dba hive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function imeiarray_clear as 'com.youzu.mob.java.udf.ImeiArrayLuhnClear';
create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';
create temporary function imei_array_union as 'com.youzu.mob.mobdi.ImeiArrayUnion';
create temporary function imei_verify AS 'com.youzu.mob.java.udf.ImeiVerify';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5120m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5120m';
set mapreduce.reduce.memory.mb=16384;
set mapreduce.reduce.java.opts='-Xmx12288m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;



insert overwrite table $dws_device_imei_di partition(day='$insert_day')
select
  device,
  concat_ws(',', collect_list(imei)) as imei,
  concat_ws(',', collect_list(imei_tm)) as imei_tm
from
(
      select
          device,
          dt,
          concat_ws(',', if(size(collect_list(imei))>0,collect_list(imei),null),if(size(collect_list(imei_arr))>0,collect_list(imei_arr),null)) as imei,
          concat_ws(',', if(size(collect_list(imei_tm))>0,collect_list(imei_tm),null),if(size(collect_list(imei_arr_tm))>0,collect_list(imei_arr_tm),null)) as imei_tm
      from
      (
          select
              device,
              CASE
                WHEN jh_blacklist_imei.value IS NOT NULL THEN null
                WHEN imei is not null and imei <> '' then imei
                ELSE null
              END as imei,
              imei_arr,
              CASE
                WHEN jh_blacklist_imei.value IS NOT NULL THEN null
                WHEN imei is not null and imei <> '' then imei_tm
                ELSE null
              END as imei_tm,
              imei_arr_tm,
              dt
          from
          (
            select
                log_info_jh.device as device,
                if(length(trim(if(luhn_checker(imei), imei, ''))) = 0 ,null,imei) as imei,
                imei_arr,
                cast(if(length(trim(if(luhn_checker(imei), imei, ''))) = 0 or imei is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as imei_tm,
                imei_arr_tm,
                dt
            from
            (
              select
                  muid as device,
                  case
                    when trim(imei) rlike '0{14,17}' then ''
                    when length(trim(lower(imei))) = 16 and trim(imei) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(imei,1,14))), ' |/|-|imei:', '')), regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),'')
                    when length(trim(lower(imei))) = 16 and trim(imei) not rlike '^[0-9]+$' then ''
                    when imei_verify(regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')) then regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')
                    else ''
                  end as imei,
                  case
                    when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                      then imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                    else null
                  end as imei_arr,
                  case
                    when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                      then imei_array_union('date',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                    else null
                  end as imei_arr_tm,
                  serdatetime,
                  dt
              from $log_device_info_jh
              where dt ='$insert_day' and plat = 1 and muid is not null and length(trim(muid))=40 and (imei is not null or imeiarray is not null)
            ) log_info_jh
          ) tt
          left join
          (SELECT lower(value) as value FROM $blacklist where type='imei' and day='20180702' GROUP BY lower(value)) jh_blacklist_imei
          on tt.imei=jh_blacklist_imei.value
      ) a group by device, dt
) tt
where imei is not null and length(trim(imei))>0
group by device
"


# 分区清理，保留最近5个分区
for old_version in `hive -e "show partitions $dws_device_imei_di" | grep -v '_bak' | sort | head -n -5`
do
  echo "rm $old_version"
  hive -e "alter table $dws_device_imei_di drop if exists partition ($old_version)"
done
