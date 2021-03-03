#!/bin/sh
set -e -x


if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

empty2null() {
f="$1"
echo "if(length($f) <= 0, null, $f)"
}

:<<!
生成dw_mobdi_md.android_id_mapping_incr增量表：
取dw_mobdi_etl.log_device_info_jh和dw_mobdi_etl.log_device_info的数据根据device去重
!

hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function imeiarray_clear as 'com.youzu.mob.java.udf.ImeiArrayClear';
create temporary function imsiarray_clear as 'com.youzu.mob.java.udf.ImsiArrayClear';
create temporary function get_mac as 'com.youzu.mob.java.udf.GetMacByWlan0';
create temporary function combine_unique as 'com.youzu.mob.java.udf.CombineUniqueUDAF';
create temporary function extract_phone_num as 'com.youzu.mob.java.udf.PhoneNumExtract';
create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';
create temporary function array_distinct as 'com.youzu.mob.java.udf.ArrayDistinct';
create temporary function imei_array_union as 'com.youzu.mob.mobdi.ImeiArrayUnion';
set hive.exec.parallel=true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';


insert overwrite table dw_mobdi_md.android_id_mapping_incr partition(day=$day)
select device,mac,macarray,imei,imeiarray,
serialno,adsid,androidid,simserialno,
phoneno,phoneno_tm,imsi,imsi_tm,imsiarray,snsuid_list,simserialno_tm,serialno_tm,mac_tm,
        imei_tm,
        adsid_tm,
        androidid_tm, array() as carrierarray,'' phone,'' phone_tm,orig_imei,orig_imei_tm,
        '' as oaid,
        '' as oaid_tm
        from (
select device,mac,macarray,orig_imei,imei,imeiarray,
serialno,adsid,androidid,simserialno,
phoneno,phoneno_tm,imsi,imsi_tm,imsiarray,snsuid_list,
    simserialno_tm,
    serialno_tm,
        mac_tm,
        imei_tm,
        orig_imei_tm,
        adsid_tm,
        androidid_tm,
    row_number() over (partition by device order by mac desc) as rank from (
select
    c.device,
    case when length(mac) = 0 then null else mac end as mac,
    case when size(macarray[0]) = 0 then null else macarray end as macarray,
    case when length(orig_imei) = 0 then null else orig_imei end as orig_imei,
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
        orig_imei_tm,
        imei_tm,
        adsid_tm,
        androidid_tm
from (
    select
        device,
        mac,
        macarray,
        orig_imei,
        imei,
        imeiarray,
        serialno,
        serialno_tm,
        adsid,
        androidid,
        split(coalesce(sort_array(simserialno_arr)[size(simserialno_arr)-1], ''), '_')[1] simserialno,
        split(coalesce(sort_array(simserialno_arr)[size(simserialno_arr)-1], ''), '_')[0] simserialno_tm,
        split(coalesce(sort_array(phoneno_arr)[size(phoneno_arr)-1], ''), '_')[1] phoneno,
        split(coalesce(sort_array(phoneno_arr)[size(phoneno_arr)-1], ''), '_')[0] phoneno_tm,
        imsi,
        imsi_tm,
        imsiarray,
        mac_tm,
        orig_imei_tm,
        imei_tm,
        adsid_tm,
        androidid_tm
    from (
        select
            device,
            concat_ws(',', collect_list(mac)) as mac,
            sort_array(collect_set(macmap)) as macarray,
            coalesce(collect_list(imei)[0],'') as orig_imei,
            concat_ws(',', if(size(collect_list(imei))>0,collect_list(imei),null),if(size(collect_list(imei_arr))>0,collect_list(imei_arr),null)) as imei,
            combine_unique(imeiarray) as imeiarray,
            concat_ws(',', collect_list(serialno)) as serialno,
            concat_ws(',', collect_list(serialno_tm)) as serialno_tm,
            concat_ws(',', collect_list(adsid)) as adsid,
            concat_ws(',', collect_list(androidid)) as androidid,
            collect_set(simserialno) as simserialno_arr,
            collect_set(phoneno) as phoneno_arr,
            concat_ws(',', collect_list(imsi)) as imsi,
            concat_ws(',', collect_list(imsi_tm)) as imsi_tm,
            combine_unique(imsiarray) as imsiarray,
            concat_ws(',', collect_list(mac_tm)) as mac_tm,
            concat_ws(',', if(size(collect_list(imei_tm))>0,collect_list(imei_tm),null)) as orig_imei_tm,
            concat_ws(',', if(size(collect_list(imei_tm))>0,collect_list(imei_tm),null),if(size(collect_list(imei_arr_tm))>0,collect_list(imei_arr_tm),null)) as imei_tm,
            concat_ws(',', collect_list(adsid_tm)) as adsid_tm,
            concat_ws(',', collect_list(androidid_tm)) as androidid_tm
            from (
            select
                device,
                CASE WHEN blacklist_mac.value IS NOT NULL THEN ''
                  WHEN regexp_replace(trim(lower(mac)), '-|\\\\.|:','') rlike '^[0-9a-f]{12}$' then substring(regexp_replace(regexp_replace(trim(lower(mac)), '-|\\\\.|:',''), '(.{2})', '\$1:'), 1, 17)
                  ELSE '' END as mac,
                macmap,
                imei,
                imei_arr,
                imeiarray,
                serialno,
                adsid,
                androidid,
                simserialno,
                phoneno,
                imsi,
                imsi_tm,
                imsiarray,
                CASE WHEN blacklist_mac.value IS NOT NULL THEN ''
                WHEN regexp_replace(trim(lower(mac)), '-|\\\\.|:','') rlike '^[0-9a-f]{12}$' then mac_tm
                ELSE '' END as mac_tm,
                imei_tm,
                imei_arr_tm,
                serialno_tm,
                adsid_tm,
                androidid_tm
            from
            (
              select
                  device,
                  case when get_mac(macarray) is not null and get_mac(macarray) <> '02:00:00:00:00:00' then lower(get_mac(macarray)) else `empty2null 'mac'` end as mac,
                  m as macmap,
                  if(luhn_checker(regexp_replace(trim(imei), ' |/|-','')), if(length(trim(imei))=0,null,regexp_replace(trim(imei), ' |/|-','')), null) as imei,
                  imeiarray_clear(imeiarray) as imeiarray,
                  case when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0 then imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                  else null end as imei_arr,
                  `empty2null 'serialno'` as serialno,
                  `empty2null 'adsid'` as adsid,
                  `empty2null 'androidid'` as androidid,
                  if(length(simserialno)=0, null, concat(serdatetime, '_', simserialno)) simserialno,
                  if(length(split(phoneno, '\\\\|')[0]) <> 17, null, concat(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'), '_', split(phoneno, '\\\\|')[0])) as phoneno,
                  `empty2null 'imsi'` as imsi,
                  cast(if(length(imsi)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as imsi_tm,
                  imsiarray_clear(imsiarray) as  imsiarray,
                  case when get_mac(macarray) is not null and get_mac(macarray) <> '02:00:00:00:00:00'  then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
                  when `empty2null 'mac'` is not null then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
                  else null end as mac_tm,
                  cast(if(luhn_checker(regexp_replace(trim(imei), ' |/|-','')),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),null)  as string) as imei_tm,
                  case when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0 then imei_array_union('date',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                  else null end as imei_arr_tm,
                  cast(if(length(serialno)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as serialno_tm,
                  cast(if(length(adsid)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as adsid_tm,
                  cast(if(length(androidid)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as androidid_tm
              from dw_mobdi_etl.log_device_info_jh as jh
              lateral view explode(coalesce(macarray, array(map()))) tf as m
              where jh.dt = '$day' and jh.plat = 1 and device is not null and length(device)=40
            ) device_info_jh
            left join
            (SELECT value FROM dm_sdk_mapping.blacklist where type='mac' and day=20180702 GROUP BY value) blacklist_mac
            on (substring(regexp_replace(regexp_replace(trim(lower(device_info_jh.mac)), '-|\\\\.|:',''), '(.{2})', '\$1:'), 1, 17)=blacklist_mac.value)
        ) as a
        group by device
    ) as b

    union all

    select
        device,
        concat_ws(',', collect_list(mac)) as mac,
        array(map()) as macarray,
        coalesce(collect_list(imei)[0],'') as orig_imei,
        concat_ws(',', collect_list(imei)) as imei,
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
                concat_ws(',', collect_list(imei_tm)) as orig_imei_tm,
        concat_ws(',', collect_list(imei_tm)) as imei_tm,
        concat_ws(',', collect_list(adsid_tm)) as adsid_tm,
        concat_ws(',', collect_list(androidid_tm)) as androidid_tm
    from (
        select
            id as device,
            `empty2null 'mac'` as mac,
            if(luhn_checker(regexp_replace(trim(udid), ' |/|-','')), if(length(trim(udid))=0,null,regexp_replace(trim(udid), ' |/|-','')), null) as imei,
            `empty2null 'adsid'` as adsid,
            `empty2null 'androidid'` as androidid,
             case when length(mac)>0 then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as  string) else null end as mac_tm,
             cast(if(luhn_checker(regexp_replace(trim(udid), ' |/|-','')),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),null) as string)  as imei_tm,
             cast(if(length(adsid)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as adsid_tm,
             cast(if(length(androidid)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as androidid_tm
        from dw_mobdi_etl.log_device_info as a
        left join (select device from dw_mobdi_etl.log_device_info_jh where dt = '$day' and plat = 1 and device is not null  and length(device)=40 group by device) as jh
        on a.id = jh.device
        where jh.device is null and a.plat = 1 and a.dt = '$day' and a.id is not null and length(a.id)=40
    ) as b
    group by device
) as c
left join dw_mobdi_md.sdk_device_snsuid_list_android as e
on (case when length(c.device)=40 then c.device else concat('',rand()) end = e.deviceid)
) as f
) as g
where g.rank = 1

"
