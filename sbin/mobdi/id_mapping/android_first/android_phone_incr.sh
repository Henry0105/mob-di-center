#!/bin/sh

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi


:<<!
更新设备的phone和imsi：
phone的来源分为三个：1 公共库 2 交换数据 3 sms数据  4 秒验数据
imsi的来源分为两个：1 公共库 2 交换数据

step1；
根据交换数据 dm_sdk_mapping.phone_mapping_full 表中全量的 imei-phone  mac-phone 中的数据与公共库的imei，mac做join更新设备的phone
step2：
根据sms数据 dm_smssdk_master.log_device_phone_dedup和公共库的device做join，更新设备的phone
step3:
根据秒验数据 mobauth 和公共库的device进行join, 更新设备的phone
step4：
根据交换数据 dm_sdk_mapping.phone_mapping_full 中全量的 phone-imsi与更新后的phone做join，更新设备的imsi
!


insert_day=$1
# 获取当前日期的下个月第一天
nextmonth=`date -d "${insert_day} +1 month" +%Y%m01`
# 获取当前日期所在月的最后一天
enddate=`date -d "$nextmonth last day" +%Y%m%d`

log_device_phone_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_smssdk_master', 'log_device_phone_dedup', 'day');
drop temporary function GET_LAST_PARTITION;
"
dedup_last_partition=(`hive -e "$log_device_phone_sql"`)

# input
ext_phone_mapping_incr=dm_mobdi_mapping.ext_phone_mapping_incr
dws_id_mapping_android_di=dm_mobdi_topic.dws_id_mapping_android_di
log_device_phone_dedup=dm_smssdk_master.log_device_phone_dedup

# 秒验数据源
mobauth_operator_login=dw_sdk_log.mobauth_operator_login
mobauth_operator_auth=dw_sdk_log.mobauth_operator_auth
mobauth_pvlog=dw_sdk_log.mobauth_pvlog

# output
dws_phone_mapping_di=dm_mobdi_topic.dws_phone_mapping_di


HADOOP_USER_NAME=dba hive -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

SET mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx8g';
set mapreduce.child.map.java.opts='-Xmx8g';
set mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts='-Xmx8g';
SET mapreduce.map.java.opts='-Xmx8g';
set mapreduce.job.queuename=root.yarn_data_compliance2;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function extract_phone_num2 as 'com.youzu.mob.java.udf.PhoneNumExtract2';
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';

--android incr
with android_imei14_exchange as (  --14位imei 对应 phone_set
  select
    imei,
    concat_ws(',',collect_list(string_sub_str(phone)) )as phone_set,
    concat_ws(',',collect_list( phone_tm )) as phone_tm_set
  from
  (
    select
      owner_data as imei,
      split(extract_phone_num2(trim(ext_data)),',')[0] as phone,
      cast(unix_timestamp(processtime,'yyyyMMdd') as string) as phone_tm
    from $ext_phone_mapping_incr
    where type = 'imei_phone'
    and length(trim(owner_data)) = 14
    and ext_data rlike '^[1][3-8]\\\d{9}$'
    and length(split(extract_phone_num2(trim(ext_data)), ',')[0]) = 17
  )imei_14
  group by imei
),


android_imei15_exchange as (  --15位imei 对应 phone_set
  select imei,
  concat_ws(',',collect_list(string_sub_str(phone)) )as phone_set,
  concat_ws(',',collect_list( phone_tm )) as phone_tm_set
  from
  (
    select
      owner_data as imei,
      split(extract_phone_num2(trim(ext_data)),',')[0] as phone,
      cast(unix_timestamp(processtime,'yyyyMMdd') as string ) as phone_tm
    from $ext_phone_mapping_incr exchange_full
    left semi join (select owner_data as imei1, count(1) as cnt from $ext_phone_mapping_incr where type = 'imei_phone'  group by owner_data having cnt < 50) filter on (filter.imei1 = exchange_full.owner_data)
    where type = 'imei_phone'
    and length(trim(owner_data)) = 15
    and ext_data rlike '^[1][3-8]\\\d{9}$'
    and length(split(extract_phone_num2(trim(ext_data)), ',')[0]) = 17
  )imei_15
  group by imei
),


android_mac_exchange as (  --mac 对应 phone_set
  select
    lower(regexp_replace(trim(owner_data),':','')) as mac,
    concat_ws(',',collect_list(string_sub_str(split(extract_phone_num2(trim(ext_data)), ',')[0]))) as phone_set,
    concat_ws(',',collect_list(cast(unix_timestamp(processtime,'yyyyMMdd') as string))) as phone_tm_set
  from $ext_phone_mapping_incr
  where type='mac_phone'
  and length(trim(owner_data)) > 0
  and ext_data rlike '^[1][3-8]\\\d{9}$'
  and length(split(extract_phone_num2(trim(ext_data)), ',')[0]) = 17
  group by lower(regexp_replace(trim(owner_data),':',''))
),


sms_phoneno as (
  select muid as device,
  concat_ws(',',collect_list(string_sub_str(phone))) as phone,
  concat_ws(',',collect_list(cast( if(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') is not null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),unix_timestamp('$enddate','yyyyMMdd')) as string))) as phone_tm
  from(
    select
      muid,
      case
        when lower(trim(phone)) rlike '[0-9a-f]{32}' then ''
        when zone in ('852','853','886','86', '1', '7', '81', '82') then split(extract_phone_num2(concat('+', zone, ' ', phone)), ',')[0]
      else ''
      end as phone,
      serdatetime
    from $log_device_phone_dedup
    where day='$dedup_last_partition'
    and length(trim(muid)) = 40
  ) t
  where length(phone)=17
  group by muid
),


mobauth_phone_v as (
    select device,
           concat_ws(',', collect_list(phone)) as mobauth_phone,
           concat_ws(',', collect_list(datetime)) as mobauth_phone_tm
    from
    (
        select device,
               phone,
               cast(max(datetime) as string) as datetime
        from
        (
            select
                muid as device,
                case
                  when lower(trim(phone)) rlike '[0-9a-f]{32}' then ''
                  when length(split(extract_phone_num2(trim(phone)), ',')[0]) = 17 then string_sub_str(split(extract_phone_num2(trim(phone)), ',')[0])
                  else ''
                end as phone,
                cast(max(datetime)/1000 as bigint) as datetime
            from $mobauth_operator_login
            where day = '20150101'
            and phone != ''
            and phone is not null
            and muid != ''
            and muid is not null
            and muid = regexp_extract(muid, '[0-9a-f]{40}', 0)
            and plat = 1
            group by muid,phone

            union all

            select
              t2.muid as device,
              t1.phone as phone,
              datetime
            from
            (
                select duid,
                       case
                          when lower(trim(phone)) rlike '[0-9a-f]{32}' then ''
                          when length(split(extract_phone_num2(trim(phone)), ',')[0]) = 17 then string_sub_str(split(extract_phone_num2(trim(phone)), ',')[0])
                          else ''
                       end as phone,
                       cast(max(datetime)/1000 as bigint) as datetime
                from $mobauth_operator_auth
                where plat = 1
                and duid is not null
                and phone is not null
                and length(duid) = 40
                and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
                and day = '20150101'
                group by duid,phone
            ) t1
            inner join
            (
                select muid,
                       duid
                from $mobauth_pvlog
                where plat = 1
                and muid is not null
                and duid is not null
                and length(muid) = 40
                and length(duid) = 40
                and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
                and muid = regexp_extract(muid, '[0-9a-f]{40}', 0)
                and day = '20150101'
                group by muid,duid
            ) t2
            on t1.duid = t2.duid
        )a group by device,phone
    )a group by device
),

ext_phoneno_v as(
select device,concat_ws(',',if(length(trim(phone_mac))=0,null,phone_mac),if(length(trim(phone_imei))=0,null,phone_imei)) as phone,
  concat_ws(',',if(length(trim(phone_mac_tm))=0,null,phone_mac_tm),if(length(trim(phone_imei_tm))=0,null,phone_imei_tm)) as phone_tm
from
(
  select android_incr_explode.device as device,
concat_ws(',',collect_list(android_mac_exchange.phone_set)) as phone_mac,
concat_ws(',',collect_list(android_mac_exchange.phone_tm_set)) as phone_mac_tm,
concat_ws(',',collect_list(android_imei15_exchange.phone_set),collect_list(android_imei14_exchange.phone_set)) as phone_imei,
concat_ws(',',collect_list(android_imei15_exchange.phone_tm_set),collect_list(android_imei14_exchange.phone_tm_set)) as phone_imei_tm
  from
  (
    select
        android_incr.device as device,
        t_mac.mac1 as mac,
        t_imei.imei1 as imei
        from
        (
          SELECT
          device,
          CASE WHEN length(trim(mac)) > 900 OR mac IS NULL then '' else mac END AS mac,
          CASE WHEN length(trim(imei)) > 900 OR imei IS NULL then '' else imei END AS imei
          FROM $dws_id_mapping_android_di
          where day='$insert_day'
          and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
        ) android_incr
        lateral view explode(split(android_incr.mac, ',')) t_mac as mac1
        lateral view explode(split(android_incr.imei, ',')) t_imei as imei1
  ) android_incr_explode
  left join android_mac_exchange
  on
  (case when length(trim(android_incr_explode.mac)) > 0 then lower(regexp_replace(trim(android_incr_explode.mac),':','')) else concat('',rand()) end= android_mac_exchange.mac)
  left join android_imei15_exchange
  on (case when length(trim(android_incr_explode.imei)) = 15
           then android_incr_explode.imei else concat('',rand()) end = android_imei15_exchange.imei)
  left join android_imei14_exchange
  on
  (case when length(trim(android_incr_explode.imei)) >= 14 then substring(android_incr_explode.imei, 1, 14) else concat('',rand()) end = android_imei14_exchange.imei)
  group by device
)tt
),

phoneno_imsi as (
    select coalesce(c.device, d.device) as device,
        phoneno,
        phoneno_tm,
        ext_phoneno,
        ext_phoneno_tm,
        sms_phoneno,
        sms_phoneno_tm,
        imsi,
        imsi_tm,
        nvl(d.mobauth_phone, null) as mobauth_phone,
        nvl(d.mobauth_phone_tm, null) as mobauth_phone_tm
    from
    (
        select
          coalesce(a.device, b.device) as device,
          nvl(a.phoneno, null) as phoneno,
          nvl(a.phoneno_tm, null) as phoneno_tm,
          nvl(a.ext_phoneno, null) as ext_phoneno,
          nvl(a.ext_phoneno_tm, null) as ext_phoneno_tm,
          nvl(b.phone, null) as sms_phoneno,
          nvl(b.phone_tm, null) as sms_phoneno_tm,
          nvl(a.imsi, null) as imsi,
          nvl(a.imsi_tm, null) as imsi_tm
        from
        (
          select android_id_incr.device as device,
             phoneno,
             phoneno_tm,
             ext_phoneno_v.phone as ext_phoneno,
             ext_phoneno_v.phone_tm as ext_phoneno_tm,
             imsi,
             imsi_tm
          from
          (
            select device,
                  phoneno,
                  phoneno_tm,
                  imsi,
                  imsi_tm
            from $dws_id_mapping_android_di
            where day='$insert_day'
            and device is not null
            and length(device)=40
            and device = regexp_extract(device,'([a-f0-9]{40})', 0)
          ) android_id_incr
          left join
          ext_phoneno_v on android_id_incr.device=ext_phoneno_v.device
        ) a
        full join
        sms_phoneno b on a.device=b.device
    ) c
    full join
    mobauth_phone_v d on c.device=d.device
),



ext_phone_imsi as (
select string_sub_str(split(extract_phone_num2(trim(owner_data)), ',')[0]) as phoneno,
     concat_ws(',',collect_list(trim(ext_data))) as imsi,
     concat_ws(',',collect_list(cast(unix_timestamp(processtime,'yyyyMMdd') as string))) as imsi_tm
from $ext_phone_mapping_incr
where type='phone_imsi'
and length(trim(ext_data)) > 0
and owner_data rlike '^[1][3-8]\\\d{9}$'
and length(split(extract_phone_num2(trim(owner_data)), ',')[0]) = 17
group by string_sub_str(split(extract_phone_num2(trim(owner_data)), ',')[0])
),


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


insert overwrite table $dws_phone_mapping_di partition(day=$insert_day,plat=1)
select phoneno_imsi.device as device,
  phoneno_imsi.phoneno as phoneno,
  phoneno_imsi.phoneno_tm as phoneno_tm,
  phoneno_imsi.ext_phoneno as ext_phoneno,
  phoneno_imsi.ext_phoneno_tm as ext_phoneno_tm,
  phoneno_imsi.sms_phoneno as sms_phoneno,
  phoneno_imsi.sms_phoneno_tm as sms_phoneno_tm,
  phoneno_imsi.imsi as imsi,
  phoneno_imsi.imsi_tm as imsi_tm,
  ext_imsi_v.ext_imsi as ext_imsi,
  ext_imsi_v.ext_imsi_tm as ext_imsi_tm,
  phoneno_imsi.mobauth_phone as mobauth_phone,
  phoneno_imsi.mobauth_phone_tm as mobauth_phone_tm
from phoneno_imsi
left join ext_imsi_v
on phoneno_imsi.device=ext_imsi_v.device
"