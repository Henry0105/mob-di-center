#!/bin/sh

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1
log_device_phone_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_smssdk_master', 'log_device_phone_dedup', 'day');
drop temporary function GET_LAST_PARTITION;
"
log_device_phone_dedup_last_snapshot_day=(`hive -e "$log_device_phone_sql"`)

:<<!
更新设备的phone和imsi：
phone的来源分为三个：1 公共库 2 交换数据 3 sms数据
imsi的来源分为两个：1 公共库 2 交换数据

step1；
根据交换数据 dm_sdk_mapping.phone_mapping_full 表中全量的 imei-phone  mac-phone 中的数据与公共库的imei，mac做join更新设备的phone
step2：
根据sms数据 dm_smssdk_master.log_device_phone_dedup和公共库的device做join，更新设备的phone
step3：
根据交换数据 dm_sdk_mapping.phone_mapping_full 中全量的 phone-imsi与更新后的phone做join，更新设备的imsi

!

hive -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function EXTRACT_PHONE_NUM as 'com.youzu.mob.java.udf.PhoneNumExtract2';
--android incr
with android_imei14_exchange as (  --14位imei 对应 phone_set
  select imei,
  concat_ws(',',collect_list(phone) )as phone_set,
  concat_ws(',',collect_list( phone_tm )) as phone_tm_set
  from
  (
    select owner_data as imei, split(EXTRACT_PHONE_NUM(trim(ext_data)),',')[0] as phone,cast(unix_timestamp(processtime,'yyyyMMdd') as string) as phone_tm
    from dm_mobdi_mapping.ext_phone_mapping_incr
    where type = 'imei_phone'
    and length(trim(owner_data)) = 14
    and ext_data rlike '^[1][3-8]\\\d{9}$'
    and length(split(EXTRACT_PHONE_NUM(trim(ext_data)), ',')[0]) = 17
  )imei_14
  group by imei
),
android_imei15_exchange as (  --15位imei 对应 phone_set
  select imei,
  concat_ws(',',collect_list(phone) )as phone_set,
  concat_ws(',',collect_list( phone_tm )) as phone_tm_set
  from
  (
    select owner_data as imei,
    split(EXTRACT_PHONE_NUM(trim(ext_data)),',')[0] as phone,
    cast(unix_timestamp(processtime,'yyyyMMdd') as string ) as phone_tm
    from dm_mobdi_mapping.ext_phone_mapping_incr exchange_full
    left semi join (select owner_data as imei1, count(1) as cnt from dm_mobdi_mapping.ext_phone_mapping_incr where type = 'imei_phone'  group by owner_data having cnt < 50) filter on (filter.imei1 = exchange_full.owner_data)
    where type = 'imei_phone'
    and length(trim(owner_data)) = 15
    and ext_data rlike '^[1][3-8]\\\d{9}$'
    and length(split(EXTRACT_PHONE_NUM(trim(ext_data)), ',')[0]) = 17
  )imei_15
  group by imei
),
android_mac_exchange as (  --mac 对应 phone_set
  select lower(regexp_replace(trim(owner_data),':','')) as mac,
  concat_ws(',',collect_list(split(EXTRACT_PHONE_NUM(trim(ext_data)), ',')[0])) as phone_set,
  concat_ws(',',collect_list(cast(unix_timestamp(processtime,'yyyyMMdd') as string))) as phone_tm_set
  from dm_mobdi_mapping.ext_phone_mapping_incr
  where type='mac_phone'
  and length(trim(owner_data)) > 0
  and ext_data rlike '^[1][3-8]\\\d{9}$'
  and length(split(EXTRACT_PHONE_NUM(trim(ext_data)), ',')[0]) = 17
  group by lower(regexp_replace(trim(owner_data),':',''))
),

sms_phoneno as (  --device对应 phone_set
  select devices_shareid as device,
  concat_ws(',',collect_list(phone)) as phone,
  concat_ws(',',collect_list(cast( if(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') is not null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),unix_timestamp('$insert_day','yyyyMMdd')) as string))) as phone_tm
  from(
  select devices_shareid,split(EXTRACT_PHONE_NUM(concat('+',zone,' ',phone)),',')[0] as phone,serdatetime
  from dm_smssdk_master.log_device_phone_dedup
  where day='$log_device_phone_dedup_last_snapshot_day'
  and devices_plat = 1 and zone in ('852','853','886','86')
  and length(trim(devices_shareid)) = 40
  and phone rlike '^[1][3-8]\\\d{9}$|^([6|9])\\\d{7}$|^[0][9]\\\d{8}$|^[6]([8|6])\\\d{5}$'
  )t
  where length(phone)=17
  group by devices_shareid
),
ext_phoneno_v as(
select device,concat_ws(',',if(length(trim(phone_mac))=0,null,phone_mac),if(length(trim(phone_imei))=0,null,phone_imei)) as phone,
  concat_ws(',',if(length(trim(phone_mac_tm))=0,null,phone_mac_tm),if(length(trim(phone_imei_tm))=0,null,phone_imei_tm)) as phone_tm
from
(
  select android_incr_explode.device,
  concat_ws(',',collect_list(android_mac_exchange.phone_set)) as phone_mac,
  concat_ws(',',collect_list(android_mac_exchange.phone_tm_set)) as phone_mac_tm,
  concat_ws(',',collect_list(android_imei15_exchange.phone_set),collect_list(android_imei14_exchange.phone_set)) as phone_imei,
  concat_ws(',',collect_list(android_imei15_exchange.phone_tm_set),collect_list(android_imei14_exchange.phone_tm_set)) as phone_imei_tm
  from
  (
    select
        android_incr.device,
        t_mac.mac1 as mac,
        t_imei.imei1 as imei
        from
        (
          SELECT
          device,
          CASE WHEN length(trim(mac)) > 900 OR mac IS NULL then '' else mac END AS mac,
          CASE WHEN length(trim(imei)) > 900 OR imei IS NULL then '' else imei END AS imei
          FROM dw_mobdi_md.android_id_mapping_incr
          where day='$insert_day'
          and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
        ) android_incr
        lateral view explode(split(android_incr.mac, ',')) t_mac as mac1
        lateral view explode(split(android_incr.imei, ',')) t_imei as imei1
  )android_incr_explode
  left join android_mac_exchange
  on
  (case when length(trim(android_incr_explode.mac)) > 0 then lower(regexp_replace(trim(android_incr_explode.mac),':','')) else concat('',rand()) end= android_mac_exchange.mac)
  left join android_imei15_exchange
  on (case when length(trim(android_incr_explode.imei)) = 15
           then android_incr_explode.imei else concat('',rand()) end = android_imei15_exchange.imei)
  left join android_imei14_exchange
  on
  (case when length(trim(android_incr_explode.imei)) >= 14
        then substring(android_incr_explode.imei, 1, 14)
        else concat('',rand()) end = android_imei14_exchange.imei)
  group by android_incr_explode.device
)tt
),
phoneno_imsi as(
select android_id_incr.device,
       android_id_incr.phoneno,
       android_id_incr.phoneno_tm,
       ext_phoneno_v.phone as ext_phoneno,
       ext_phoneno_v.phone_tm as ext_phoneno_tm,
       sms_phoneno.phone as sms_phoneno,
       sms_phoneno.phone_tm as sms_phoneno_tm,
       android_id_incr.imsi,
       android_id_incr.imsi_tm
FROM dw_mobdi_md.android_id_mapping_incr android_id_incr
left join
ext_phoneno_v on android_id_incr.device=ext_phoneno_v.device
left join
sms_phoneno on android_id_incr.device = sms_phoneno.device
where android_id_incr.day='$insert_day'
and android_id_incr.device is not null and length(android_id_incr.device)= 40 and android_id_incr.device = regexp_extract(android_id_incr.device,'([a-f0-9]{40})', 0)
),
ext_phone_imsi as (  --phone对应的imsi_set
  select split(EXTRACT_PHONE_NUM(trim(owner_data)), ',')[0] as phoneno,
         concat_ws(',',collect_list(trim(ext_data))) as imsi,
         concat_ws(',',collect_list(cast(unix_timestamp(processtime,'yyyyMMdd') as string))) as imsi_tm
  from dm_mobdi_mapping.ext_phone_mapping_incr
  where type='phone_imsi'
  and length(trim(ext_data)) > 0
  and owner_data rlike '^[1][3-8]\\\d{9}$'
  and length(split(EXTRACT_PHONE_NUM(trim(owner_data)), ',')[0]) = 17
  group by split(EXTRACT_PHONE_NUM(trim(owner_data)), ',')[0]
)
,
ext_imsi_v as(
select phone_explode.device,
       concat_ws(',',collect_list(ext_phone_imsi.imsi)) as ext_imsi,
       concat_ws(',',collect_list(ext_phone_imsi.imsi_tm)) as ext_imsi_tm
from
(
  select device,t_phone.phoneno  as phoneno from phoneno_imsi  lateral view explode(split(concat_ws(',',phoneno,ext_phoneno,sms_phoneno),',')) t_phone as phoneno
) phone_explode
left join
ext_phone_imsi on (case when length(trim(phone_explode.phoneno))>0
          then phone_explode.phoneno else concat('',rand()) end = ext_phone_imsi.phoneno )
group by device
)
insert overwrite table dw_mobdi_md.phone_mapping_incr partition(day=$insert_day,plat=1)
select phoneno_imsi.*,ext_imsi_v.ext_imsi,ext_imsi_v.ext_imsi_tm
from phoneno_imsi
left join ext_imsi_v
on phoneno_imsi.device=ext_imsi_v.device
"
