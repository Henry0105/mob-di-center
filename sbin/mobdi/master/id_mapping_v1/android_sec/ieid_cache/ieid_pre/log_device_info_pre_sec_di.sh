#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1

# input
dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

# mapping
sysver_mapping_par=dm_sdk_mapping.sysver_mapping_par

# output
log_device_info_pre_sec_di=dm_mobdi_tmp.log_device_info_pre_sec_di


## 新增数据清洗
HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function arrayDistinct as 'com.youzu.mob.java.udf.ArrayDistinct';
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5200m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $log_device_info_pre_sec_di partition(day='$day')
select device, factory, model, ieid, serdatetime, sysver
from
(
  select device, factory, model, ieid, serdatetime,
  case
    when lower(trim(info.sysver)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other', '未知') or info.sysver is null then 'unknown'
    when lower(info.sysver) not rlike '^([0-9a-z]|\\.|-| )+$' then 'unknown'
    when info.sysver = sysver_mapping.vernum then sysver_mapping.sysver
    when concat(split(regexp_replace(lower(info.sysver), 'android| ', ''), '\\.')[0], '.', split(regexp_replace(lower(info.sysver), 'android| ', ''), '\\.')[1]) in ('1.0', '1.1', '1.5', '1.6', '2.0', '2.1', '2.2', '2.3', '3.0', '3.1', '3.2', '4.0', '4.1', '4.2', '4.3', '4.4', '4.4w', '5.0', '5.1', '6.0', '7.0', '7.1', '8.0', '8.1', '9.0', '10.0', '11.0') then concat(split(regexp_replace(lower(info.sysver), 'android| ', ''), '\\.')[0], '.', split(regexp_replace(lower(info.sysver), 'android| |w', ''), '\\.')[1])
    else 'unknown'
  end as sysver
  from
  (
    select device, serdatetime,
    case
      when (length(trim(ieid)) <= 0 or ieid is null) and (length(trim(ieid_arr)) <= 0 or ieid_arr is null) then null
      when (length(trim(ieid)) <= 0 or ieid is null) and length(trim(ieid_arr)) > 0 then trim(ieid_arr)
      when length(trim(ieid)) > 0 and (length(trim(ieid_arr)) <= 0 or ieid_arr is null)then trim(ieid)
    else concat(trim(ieid), ',', trim(ieid_arr))
    end as ieid, factory, model, sysver, day
    from
    (
      select muid as device, unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss') as serdatetime,
      if(length(trim(ieid))=0, null, ieid) as ieid,
      nvl(ieidarray, array()) as ieidarray,
      if(length(concat_ws(',',arrayDistinct(ieidarray)))>30,concat_ws(',',arrayDistinct(ieidarray)) ,null) as ieid_arr,
      factory, model, sysver, day
      from $dwd_log_device_info_jh_sec_di as jh
      where jh.day ='$day' and jh.plat = 1 and muid is not null and length(muid) = 40 and muid = regexp_extract(muid, '([a-f0-9]{40})', 0) and breaked = false
     ) as m
  ) as info
  left join
  (
    select *
    from $sysver_mapping_par
    where version = '1004'
  ) as sysver_mapping
  on info.sysver = sysver_mapping.vernum
) as a
where device is not null and length(device) = 40 and device = regexp_extract(device, '([a-f0-9]{40})', 0) and ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other')) or (ieid is not null and trim(ieid) not in ('', ',')))
group by device, factory, model, ieid, serdatetime, sysver;
"