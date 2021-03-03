#!/bin/sh

set -e -x


if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1

#input
device_oaid_incr="dm_mobdi_mapping.device_oaid_mapping_incr"

#output
android_id_mapping_incr="dw_mobdi_md.android_id_mapping_incr"


hive -e "
SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function ARRAY_DISTINCT as 'com.youzu.mob.java.udf.ArrayDistinct';
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with android_device_carrierarray as (  --device对应的 carrier array
  select
  t1.device,
  collect_set(carrier_mapping.operator) as carrierarray
  from
  (
    select device, imsi1
    from dw_mobdi_md.android_id_mapping_incr
    lateral view explode(ARRAY_DISTINCT(split(imsi, ','), imsiarray)) t_imsi as imsi1
    where day='$insert_day' and size(ARRAY_DISTINCT(split(imsi, ','), imsiarray)) > 0
  ) t1
  inner join (select mcc_mnc, operator from dm_sdk_mapping.mapping_carrier_country) carrier_mapping on (substring(t1.imsi1, 0, 5) = carrier_mapping.mcc_mnc)
  group by t1.device
),
android_device_phone as (
select t1.device, concat_ws(',',t2.phoneno,t2.ext_phoneno,t2.sms_phoneno,mobauth.phone) as phone,
  concat_ws(',',t2.phoneno_tm,t2.ext_phoneno_tm,t2.sms_phoneno_tm,mobauth.phone_tm) as phone_tm,
  t2.imsi,t2.imsi_tm
  from (
    select case when device is not null or length(trim(device)) > 0 then device else concat('',rand()) end as device from dw_mobdi_md.android_id_mapping_incr  where day = $insert_day
  ) t1
  left join
  (
    select device,string_sub_str(phoneno) as phoneno,
    NVL(phoneno_tm,'') as phoneno_tm,
    string_sub_str(ext_phoneno) as ext_phoneno,
    NVL(ext_phoneno_tm,'') as ext_phoneno_tm,
    string_sub_str(sms_phoneno) as sms_phoneno,
    NVL(sms_phoneno_tm,'') as sms_phoneno_tm,
    concat_ws(',',imsi,ext_imsi) as imsi,
    concat_ws(',',imsi_tm,ext_imsi_tm) as imsi_tm
    from dw_mobdi_md.phone_mapping_incr
    where day=$insert_day and plat =1
  )t2
  on t1.device = t2.device
   left join
  (
     select
          deviceid as device,
          concat_ws(',',collect_list(if(phone is not null and phone REGEXP '.*[a-z]+.*' and length(phone)=32,'',phone))) as phone,
          concat_ws(',',collect_list(substring(cast(datetime as string),1,10))) as phone_tm
        from
          dw_sdk_log.mobauth_operator_login
        where
          day = $insert_day
          and phone != ''
          and deviceid != ''
          and deviceid is not null
          and plat = 1
        group by
          deviceid
  ) mobauth on t1.device =  mobauth.device
),
android_device_oaid as (
select device,
concat_ws(',', collect_list(oaid)) as oaid,
concat_ws(',', collect_list(oaid_tm)) as oaid_tm from
(
  select device,oaid,max(oaid_tm) as oaid_tm  from $device_oaid_incr where day='$insert_day' and device is not null
  group by device,oaid
)a group by device
)

insert overwrite table $android_id_mapping_incr partition(day=$insert_day)
select
    coalesce(android_incr.device,android_device_oaid.device) as device,
    mac,
    macarray,
    imei,
    imeiarray,
    serialno,
    adsid,
    androidid,
    simserialno,
    phoneno,
    phoneno_tm,
    android_device_phone.imsi,
    android_device_phone.imsi_tm,
    imsiarray,
    snsuid_list,
    simserialno_tm,
    serialno_tm,
    mac_tm,
    imei_tm,
    adsid_tm,
    androidid_tm,
    case when android_device_carrierarray.carrierarray is null or size(android_device_carrierarray.carrierarray) = 0 then null else android_device_carrierarray.carrierarray end as carrierarray,
    android_device_phone.phone,
    android_device_phone.phone_tm,
    orig_imei,
    orig_imei_tm,
    android_device_oaid.oaid,
    android_device_oaid.oaid_tm
from (select * from dw_mobdi_md.android_id_mapping_incr where day=$insert_day) android_incr
left join
android_device_carrierarray on ((case when android_incr.device is not null or length(trim(android_incr.device)) > 0 then android_incr.device else concat('', rand()) end) = android_device_carrierarray.device)
left join android_device_phone on ((case when android_incr.device is not null or length(trim(android_incr.device)) > 0 then android_incr.device else concat('', rand()) end) = android_device_phone.device)
full join android_device_oaid on ((case when android_incr.device is not null or length(trim(android_incr.device)) > 0 then android_incr.device else concat('', rand()) end) = android_device_oaid.device)

"