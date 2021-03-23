#!/bin/sh
set -e -x




#day=$1
## 获取当前日期的下个月第一天
#nextmonth=`date -d "${day} +1 month" +%Y%m01`
## 获取当前日期所在月的第一天
#startdate=`date -d"${day}" +%Y%m01`
## 获取当前日期所在月的最后一天
#enddate=`date -d "$nextmonth last day" +%Y%m%d`



# input
log_device_info_jh=dw_sdk_log.log_device_info_jh
log_device_info=dw_sdk_log.log_device_info
blacklist=dm_sdk_mapping.blacklist
dws_device_snsuid_list_android=dm_mobdi_topic.dws_device_snsuid_list_android

# output
dws_id_mapping_android_di=dm_mobdi_topic.dws_id_mapping_android_di


empty2null() {
f="$1"
echo "if(length($f) <= 0, null, $f)"
}

HADOOP_USER_NAME=dba hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function imeiarray_clear as 'com.youzu.mob.java.udf.ImeiArrayLuhnClear';
create temporary function imsiarray_clear as 'com.youzu.mob.java.udf.ImsiArrayClear';
create temporary function get_mac as 'com.youzu.mob.java.udf.GetMacByWlan0';
create temporary function combine_unique as 'com.youzu.mob.java.udf.CombineUniqueUDAF';
create temporary function extract_phone_num as 'com.youzu.mob.java.udf.PhoneNumExtract';
create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';
create temporary function array_distinct as 'com.youzu.mob.java.udf.ArrayDistinct';
create temporary function imei_array_union as 'com.youzu.mob.mobdi.ImeiArrayUnion';
create temporary function imei_verify AS 'com.youzu.mob.java.udf.ImeiVerify';
create temporary function extract_phone_num2 as 'com.youzu.mob.java.udf.PhoneNumExtract2';
create temporary function extract_phone_num3 as 'com.youzu.mob.java.udf.PhoneNumExtract3';
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';
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


insert overwrite table $dws_id_mapping_android_di partition(day='20151231')
select
    c.device as device,
    case when length(mac) = 0 then null else mac end as mac,
    case when size(macarray[0]) = 0 then null else macarray end as macarray,
    case when length(imei) = 0 then null else imei end as imei,
    case when size(imeiarray) = 0 then null else imeiarray end as imeiarray,
    case when length(serialno) = 0 then null else serialno end as serialno,
    case when length(adsid) = 0 then null else adsid end as adsid,
    case when length(androidid) = 0 then null else androidid end as androidid,
    case when length(simserialno) = 0 then null else simserialno end as simserialno,
    case when length(phoneno) = 0 then null else phoneno end as phoneno,
    case when length(phoneno_tm) = 0 then null else phoneno_tm end as phoneno_tm,
    case when length(imsi) = 0 then null else imsi end as imsi,
    case when length(imsi_tm) = 0 then null else imsi_tm end as imsi_tm,
    case when size(imsiarray) = 0 then null else imsiarray end as imsiarray,
    e.snsuid_list as snsuid_list,
    simserialno_tm,
    serialno_tm,
    mac_tm,
    imei_tm,
    adsid_tm,
    androidid_tm,
    array() as carrierarray,
    null as phone,
    null as phone_tm,
case when length(orig_imei) = 0 then null else orig_imei end as orig_imei,
    orig_imei_tm,
    null as oaid,
    null as oaid_tm
from (
    select
        device,
        concat_ws(',', collect_list(mac)) as mac,
        array(map()) as macarray,
        coalesce(collect_list(imei)[0],'') as orig_imei,
        concat_ws(',', if(size(collect_list(imei))>0, collect_list(imei),null)) as imei,
        array() as imeiarray,
        '' as serialno,
        '' as serialno_tm,
        concat_ws(',', collect_list(adsid)) as adsid,
        concat_ws(',', collect_list(androidid)) as androidid,
        '' as simserialno,
        '' as simserialno_tm,
        '' as phoneno,
        '' as phoneno_tm,
        '' imsi,
        '' as imsi_tm,
        array() as imsiarray,
        concat_ws(',', collect_list(mac_tm)) as mac_tm,
        concat_ws(',', if(size(collect_list(imei_tm))>0, collect_list(imei_tm)[0],null)) as orig_imei_tm,
        concat_ws(',', if(size(collect_list(imei_tm))>0, collect_list(imei_tm),null)) as imei_tm,
        concat_ws(',', collect_list(adsid_tm)) as adsid_tm,
        concat_ws(',', collect_list(androidid_tm)) as androidid_tm
    from (
        select
          device,

          CASE
            WHEN log_blacklist_mac.value IS NOT NULL THEN null
            WHEN mac is not null and mac <> '' then mac
            ELSE null
          END as mac,

          CASE
            WHEN log_blacklist_imei.value IS NOT NULL THEN null
            WHEN luhn_checker(imei) and length(imei) > 0 then imei
            ELSE null
          END as imei,

          if (length(adsid)=0, null, adsid) as adsid,
          if (length(androidid)=0, null, androidid) as androidid,

          CASE
            WHEN log_blacklist_mac.value IS NOT NULL THEN null
            WHEN mac is not null and mac <> '' then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
            ELSE null
          END as mac_tm,

          CASE
            WHEN log_blacklist_imei.value IS NOT NULL THEN null
            WHEN luhn_checker(imei) and length(imei) > 0 then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
            ELSE null
          END as imei_tm,

          cast(if(length(adsid)=0 or adsid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as adsid_tm,
          cast(if(length(androidid)=0 or androidid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as androidid_tm
        from
        (
          select
              a.muid as device,

              case
                when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
                when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
                when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
                then substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17)
                else ''
              end as mac,

              case
                when trim(udid) rlike '0{14,17}' then ''
                when length(trim(lower(udid))) = 16 and trim(udid) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(udid,1,14))), ' |/|-|imei:', '')), regexp_replace(trim(lower(udid)), ' |/|-|imei:', ''),'')
                when length(trim(lower(udid))) = 16 and trim(udid) not rlike '^[0-9]+$' then ''
                when imei_verify(regexp_replace(trim(lower(udid)), ' |/|-|imei:', '')) then regexp_replace(trim(lower(udid)), ' |/|-|imei:', '')
                else ''
              end as imei,

              case
                when lower(trim(adsid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or adsid is null then ''
                when regexp_replace(lower(trim(adsid)), ' |-|\\\\.|:|\073','') rlike '0{32}' then ''
                when regexp_replace(lower(trim(adsid)), ' |-|\\\\.|:|\073','') rlike '^[0-9a-f]{32}$'
                then concat
                (
                  substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 1, 8), '-',
                  substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 9, 4), '-',
                  substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 13, 4), '-',
                  substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 17, 4), '-',
                  substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 21, 12)
                )
                else ''
              end as adsid,

              case
                when lower(trim(androidid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or androidid is null then ''
                when lower(trim(androidid)) rlike '^[0-9a-f]{14,16}$' then lower(trim(androidid))
                else ''
              end as androidid,
              serdatetime
          from $log_device_info as a
          where a.plat = 1 and a.dt >= '20150514' and a.dt <='20151231' and a.muid is not null and length(a.muid)=40
        ) tt
        left join
         (SELECT lower(value) as value FROM $blacklist where type='mac' and day='20180702' GROUP BY lower(value)) log_blacklist_mac
            on (substring(regexp_replace(regexp_replace(trim(lower(tt.mac)), ' |-|\\\\.|:|\073',''), '(.{2})', '\$1:'), 1, 17)=log_blacklist_mac.value)
        left join
         (SELECT lower(value) as value FROM $blacklist where type='imei' and day='20180702' GROUP BY lower(value)) log_blacklist_imei
            on tt.imei=log_blacklist_imei.value

    ) as b
    group by device
) as c
left join $dws_device_snsuid_list_android as e
on (case when length(c.device)=40 then c.device else concat('',rand()) end = e.deviceid)
"
