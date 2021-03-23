#!/bin/sh

set -e -x

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

# 获取当前日期的下个月第一天
nextmonth=`date -d "${insert_day} +1 month" +%Y%m01`
# 获取当前日期所在月的第一天
startdate=`date -d"${insert_day}" +%Y%m01`
# 获取当前日期所在月的最后一天
enddate=`date -d "$nextmonth last day" +%Y%m%d`

#input
device_oaid_mapping_incr=dm_mobdi_mapping.dim_device_oaid_mapping_di
phone_mapping_incr=dm_mobdi_topic.dws_phone_mapping_di
mapping_carrier_country=dm_sdk_mapping.mapping_carrier_country
dws_id_mapping_android_di=dm_mobdi_topic.dws_id_mapping_android_di

#output
dws_id_mapping_android_di=dm_mobdi_topic.dws_id_mapping_android_di

HADOOP_USER_NAME=dba hive -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
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
set mapreduce.job.queuename=root.yarn_data_compliance2;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function ARRAY_DISTINCT as 'com.youzu.mob.java.udf.ArrayDistinct';



with android_device_carrierarray as (  --device对应的 carrier array
  select
    t1.device,
    collect_set(carrier_mapping.operator) as carrierarray
  from
  (
    select device, imsi1
    from $dws_id_mapping_android_di
    lateral view explode(ARRAY_DISTINCT(split(imsi, ','), imsiarray)) t_imsi as imsi1
    where day ='$insert_day' and size(ARRAY_DISTINCT(split(imsi, ','), imsiarray)) > 0
  ) t1
  inner join (select mcc_mnc, operator from $mapping_carrier_country) carrier_mapping on (substring(t1.imsi1, 0, 5) = carrier_mapping.mcc_mnc)
  group by t1.device
),



android_device_phone as (
  select
      t1.device,
      concat_ws(',',t2.phoneno,t2.ext_phoneno,t2.sms_phoneno,mobauth_phone) as phone,
      concat_ws(',',t2.phoneno_tm,t2.ext_phoneno_tm,t2.sms_phoneno_tm,mobauth_phone_tm) as phone_tm,
      t2.imsi,
      t2.imsi_tm
  from (
    select case when device is not null or length(trim(device)) > 0 then device else concat('',rand()) end as device
    from $dws_id_mapping_android_di
    where day ='$insert_day'
  ) t1
  left join
  (
    select
        device,
        phoneno,
        phoneno_tm,
        ext_phoneno,
        ext_phoneno_tm,
        sms_phoneno,
        sms_phoneno_tm,
        concat_ws(',',imsi,ext_imsi) as imsi,
        concat_ws(',',imsi_tm,ext_imsi_tm) as imsi_tm,
        mobauth_phone,
        mobauth_phone_tm
    from $phone_mapping_incr
    where day ='$insert_day'
    and plat =1
  )t2
  on t1.device = t2.device
),



android_device_oaid as (
  select
      device,
      concat_ws(',', collect_list(oaid)) as oaid,
      concat_ws(',', collect_list(oaid_tm)) as oaid_tm from
  (
    select
        device,
        oaid,
        max(oaid_tm) as oaid_tm
    from $device_oaid_mapping_incr
    where day ='$insert_day'
    and device is not null
    group by device,oaid
  )a group by device
)

insert overwrite table $dws_id_mapping_android_di partition(day=$insert_day)
select
    coalesce(e.device, f.device) as device,
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
    imsi,
    imsi_tm,
    imsiarray,
    snsuid_list,
    simserialno_tm,
    serialno_tm,
    mac_tm,
    imei_tm,
    adsid_tm,
    androidid_tm,
    carrierarray,
    phone,
    phone_tm,
    orig_imei,
    orig_imei_tm,
    f.oaid,
    f.oaid_tm
from
(
    select
        coalesce(c.device, d.device) as device,
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
        d.imsi,
        d.imsi_tm,
        imsiarray,
        snsuid_list,
        simserialno_tm,
        serialno_tm,
        mac_tm,
        imei_tm,
        adsid_tm,
        androidid_tm,
        carrierarray,
        d.phone,
        d.phone_tm,
        orig_imei,
        orig_imei_tm
    from
    (
      select
        a.device,
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
        imsiarray,
        snsuid_list,
        simserialno_tm,
        serialno_tm,
        mac_tm,
        imei_tm,
        adsid_tm,
        androidid_tm,
        case when b.carrierarray is null or size(b.carrierarray) = 0 then null else b.carrierarray end as carrierarray,
        orig_imei,
        orig_imei_tm
      from
      (
        select *
        from $dws_id_mapping_android_di
        where day ='$insert_day'
      ) a
      left join android_device_carrierarray b
      on ((case when a.device is not null or length(trim(a.device)) > 0 then a.device else concat('', rand()) end) = b.device)
    ) c
    full join android_device_phone d
    on ((case when c.device is not null or length(trim(c.device)) > 0 then c.device else concat('', rand()) end) = d.device)
) e
full join android_device_oaid f
on ((case when e.device is not null or length(trim(e.device)) > 0 then e.device else concat('', rand()) end) = f.device)
"
