#!/bin/bash

set -x -e


empty2null() {
f="$1"
echo "if(length(trim($f)) <= 0, null, $f)"
}



HADOOP_USER_NAME=dba hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function extract_phone_num2 as 'com.youzu.mob.java.udf.PhoneNumExtract2';
create temporary function extract_phone_num3 as 'com.youzu.mob.java.udf.PhoneNumExtract3';
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';
create temporary function get_mac as 'com.youzu.mob.java.udf.GetMacByWlan0';
create temporary function combine_unique as 'com.youzu.mob.java.udf.CombineUniqueUDAF';
create temporary function extract_phone_num as 'com.youzu.mob.java.udf.PhoneNumExtract';
create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';
create temporary function array_distinct as 'com.youzu.mob.java.udf.ArrayDistinct';
create temporary function imei_array_union as 'com.youzu.mob.mobdi.ImeiArrayUnion';
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
create temporary function imeiarray_clear as 'com.youzu.mob.java.udf.ImeiArrayClear';
create temporary function imsiarray_clear as 'com.youzu.mob.java.udf.ImsiArrayClear';
create temporary function imeiarray_clear as 'com.youzu.mob.java.udf.ImeiArrayLuhnClear';
create temporary function imei_verify AS 'com.youzu.mob.java.udf.ImeiVerify';


set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set hive.exec.parallel=true;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3g';
set mapreduce.map.java.opts='-Xmx3g';
SET hive.auto.convert.join=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;

drop table if exists mobdi_test.gai_device_phone_imei_mac_2020_after;
create table mobdi_test.gai_device_phone_imei_mac_2020_after as
select device, dt,
max(serdatetime) as serdatetime,
concat_ws(',', collect_set(mac)) as mac,
case
  when size(collect_set(imei)) <= 0 and size(combine_unique(split(trim(imei_arr), ','))) <= 0 then null
  when size(collect_set(imei)) <= 0 and size(combine_unique(split(trim(imei_arr), ','))) > 0 then concat_ws(',', combine_unique(split(trim(imei_arr),',')))
  when size(collect_set(imei)) > 0 and size(combine_unique(split(trim(imei_arr), ','))) <= 0 then concat_ws(',', collect_set(imei))
else concat(concat_ws(',', collect_set(imei)),',',concat_ws(',', combine_unique(split(trim(imei_arr),','))))
end as imei,
split(max(phoneno), '=')[1] as phoneno
from
(
  select muid as device, unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss') as serdatetime,
  case
    when get_mac(macarray) is not null and get_mac(macarray) <> '02:00:00:00:00:00'
	then 
	    case
          when trim(lower(get_mac(macarray))) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or get_mac(macarray) is null then ''
          when regexp_replace(trim(lower(get_mac(macarray))), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
          when regexp_replace(trim(lower(get_mac(macarray))), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
          then substring(regexp_replace(regexp_replace(trim(lower(get_mac(macarray))), ' |-|\\\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17)
          else ''
        end
    else 
	    case
          when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
          when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
          when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
          then lower(substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17))
          else ''
        end
  end as mac,
  case
    when trim(imei) rlike '0{14,17}' then ''
    when length(trim(lower(imei))) = 16 and trim(imei) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(imei,1,14))), ' |/|-|imei:', '')), regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),'')
    when length(trim(lower(imei))) = 16 and trim(imei) not rlike '^[0-9]+$' then ''
    when imei_verify(regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')) then 
	    if(length(trim(if(luhn_checker(regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')),regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),''))) = 0 ,null,imei)
    else ''
  end as imei,
  case
    when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
      then imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
    else null
  end as imei_arr,
  case
    when lower(trim(phoneno)) rlike '[0-9a-f]{32}' then ''
    when length(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialno), trim(cast(carrier as string)),trim(imsi))), ',')[0]) = 17
    then if (length(string_sub_str(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialno), trim(cast(carrier as string)), trim(imsi))), ',')[0]))=0, null,concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '=', split(string_sub_str(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialno), trim(cast(carrier as string)), trim(imsi))), ',')[0]), '\\\\|')[0]))
    else ''
   end as phoneno, dt
  from dw_sdk_log.log_device_info_jh as jh
  where jh.dt >= '20200101' and jh.dt < '20210801' and jh.plat = 1 and muid is not null and length(muid) = 40 and muid = regexp_extract(muid, '([a-f0-9]{40})', 0)

  union all

  select muid as device, unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as serdatetime,
  case
    when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
    when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
    when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
    then lower(substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17))
    else ''
  end as mac,
  case
    when trim(udid) rlike '0{14,17}' then ''
    when length(trim(lower(udid))) = 16 and trim(udid) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(udid,1,14))), ' |/|-|udid:', '')), regexp_replace(trim(lower(udid)), ' |/|-|udid:', ''),'')
    when length(trim(lower(udid))) = 16 and trim(udid) not rlike '^[0-9]+$' then ''
    when imei_verify(regexp_replace(trim(lower(udid)), ' |/|-|udid:', '')) then 
	    if(length(trim(if(luhn_checker(regexp_replace(trim(lower(udid)), ' |/|-|udid:', '')),regexp_replace(trim(lower(udid)), ' |/|-|udid:', ''),''))) = 0 ,null,udid)
    else ''
  end as imei,
  null as imei_arr, null as phoneno, info.dt
  from dw_sdk_log.log_device_info as info
  left join
  (
    select muid as device, dt
    from dw_sdk_log.log_device_info_jh
    where dt >= '20200101' and dt < '20210801' and plat = 1 and muid is not null and length(muid) = 40 and muid = regexp_extract(muid, '([a-f0-9]{40})', 0)
    group by muid, dt
  ) as jh
  on info.muid = jh.device and info.dt = jh.dt
  where info.dt >= '20200101' and info.dt < '20210801' and info.plat = 1 and muid is not null and length(muid) = 40 and jh.device is null
) as tt
where device is not null and length(device) = 40 and device = regexp_extract(device, '([a-f0-9]{40})', 0) and ((mac is not null and mac <> '') or (phoneno is not null and phoneno <> '') or (imei is not null and imei <> ''))
group by device, dt;
"

