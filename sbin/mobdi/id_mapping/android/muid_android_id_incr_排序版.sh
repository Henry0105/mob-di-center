#!/bin/sh
set -e -x


if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

:<<!
生成dw_mobdi_md.android_id_mapping_incr增量表：
取dw_mobdi_etl.log_device_info_jh和dw_mobdi_etl.log_device_info的数据根据device去重

具体详情：
1. 清洗log_device_info_jh 这个表里面的字段信息，imei做15位长度验证，mac会优先选取macarray里面waln0并验证mac的准确性，如果没有就以实际获取到的mac为准。获取数据的时间格式调整，及黑名单数据的过滤。
2. 以device为准整合imei，mac，serialno，phone等信息，同时计算imei，mac，serialno，phone对应的首次时间及最新活跃时间。
3. 调整数据统一数据的入库格式
4. 按device去重，以mac为排序条件，选取第一个
5. log_device_info这表处理与log_device_info_jh类似，不过log_device_info很多信息为空所以处理较为简单，同时log_device_info这表在进入mobdi时已经过滤黑名单，不需要再次过滤。
6. 两个表处理完成后通过进行union合并，当两个表出现相同的device时，以log_device_info_jh为准。
7. 以上过程最终形成表android_id_mapping_incr

muid版本：
1. 入库时，将 muid 以 device 为别名 入库
2. 从dw_mobdi_etl.log_device_info_jh和dw_mobdi_etl.log_device_info取数时，
   对字段的校验和过滤以wiki：  http://c.mob.com/pages/viewpage.action?pageId=47416256 为准
!


day=$1


# input
log_device_info_jh=dw_sdk_log.log_device_info_jh
log_device_info=dw_sdk_log.log_device_info
blacklist=dm_sdk_mapping.blacklist
dws_device_snsuid_list_android=dm_mobdi_topic.dws_device_snsuid_list_android

# output
dwd_id_mapping_android_di=dm_mobdi_master.dwd_id_mapping_android_di


empty2null() {
f="$1"
echo "if(length($f) <= 0, null, $f)"
}

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


insert overwrite table $dwd_id_mapping_android_di partition(day=$day)
select muid as device,mac,macarray,imei,imeiarray,
serialno,adsid,androidid,simserialno,
phoneno,phoneno_tm,imsi,imsi_tm,imsiarray,snsuid_list,simserialno_tm,serialno_tm,mac_tm,
        imei_tm,
        adsid_tm,
        androidid_tm, array() as carrierarray,null as phone,null as phone_tm,orig_imei,orig_imei_tm,
        null as oaid,
        null as oaid_tm
        from (
select muid,mac,macarray,orig_imei,imei,imeiarray,
serialno,adsid,androidid,simserialno,
phoneno,phoneno_tm,imsi,imsi_tm,imsiarray,snsuid_list,
    simserialno_tm,
    serialno_tm,
        mac_tm,
        imei_tm,
        orig_imei_tm,
        adsid_tm,
        androidid_tm,
    row_number() over (partition by muid order by mac desc) as rank from (
select
    c.muid,
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
        muid,
        mac,
        macarray,
        orig_imei,
        imei,
        imeiarray,
        serialno,
        serialno_tm,
        adsid,
        androidid,
        simserialno,
        simserialno_tm,
        phoneno,
        imsi,
        imsi_tm,
        imsiarray,
        mac_tm,
        orig_imei_tm,
        imei_tm,
        adsid_tm,
        androidid_tm,
        phoneno_tm
    from (
        select
            muid,
            concat_ws(',', collect_list(mac)) as mac,
            sort_array(collect_set(macmap)) as macarray,
            coalesce(collect_list(imei)[0],'') as orig_imei,
            concat_ws(',', if(size(collect_list(imei))>0,collect_list(imei),null),if(size(collect_list(imei_arr))>0,collect_list(imei_arr),null)) as imei,
            combine_unique(imeiarray) as imeiarray,
            concat_ws(',', collect_list(serialno)) as serialno,
            concat_ws(',', collect_list(serialno_tm)) as serialno_tm,
            concat_ws(',', collect_list(adsid)) as adsid,
            concat_ws(',', collect_list(androidid)) as androidid,
            concat_ws(',', collect_list(simserialno)) as simserialno,
            concat_ws(',', collect_list(phoneno)) as phoneno,
            concat_ws(',', collect_list(imsi)) as imsi,
            concat_ws(',', collect_list(imsi_tm)) as imsi_tm,
            combine_unique(imsiarray) as imsiarray,
            concat_ws(',', collect_list(mac_tm)) as mac_tm,
            concat_ws(',', if(size(collect_list(imei_tm))>0,collect_list(imei_tm),null)) as orig_imei_tm,
            concat_ws(',', if(size(collect_list(imei_tm))>0,collect_list(imei_tm),null),if(size(collect_list(imei_arr_tm))>0,collect_list(imei_arr_tm),null)) as imei_tm,
            concat_ws(',', collect_list(adsid_tm)) as adsid_tm,
            concat_ws(',', collect_list(androidid_tm)) as androidid_tm,
            concat_ws(',', collect_list(simserialno_tm)) as simserialno_tm,
            concat_ws(',', collect_list(phoneno_tm)) as phoneno_tm
            from (
            select
                muid,
                CASE
                  WHEN blacklist_mac.value IS NOT NULL THEN ''
                  WHEN mac is not null and mac <> '' then mac
                  ELSE ''
                END as mac,
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
                CASE
                  WHEN blacklist_mac.value IS NOT NULL THEN ''
                  WHEN mac is not null then mac_tm
                  ELSE ''
                END as mac_tm,
                imei_tm,
                imei_arr_tm,
                serialno_tm,
                adsid_tm,
                androidid_tm,
                simserialno_tm
                phoneno_tm
            from
            (
              select
                  muid,

                  case
                    when get_mac(macarray) is not null and get_mac(macarray) <> '02:00:00:00:00:00' then lower(get_mac(macarray))
                    when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
                    when regexp_replace(trim(lower(mac)), ' |-|\\.|:|\073', '') in ('000000000000', '020000000000') then ''
                    when regexp_replace(trim(lower(mac)), ' |-|\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
                    then substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17)
                    else ''
                  end as mac,

                  m as macmap,

                  case
                    when trim(imei) rlike '0{14,17}' then ''
                    when length(trim(lower(imei))) = 16 and trim(imei) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(imei,1,14))), ' |/|-|imei:', '')), regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),'')
                    when length(trim(lower(imei))) = 16 and trim(imei) not rlike '^[0-9]+$' then ''
                    when imei_verify(regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')) then regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')
                    else ''
                  end as imei,

                  imeiarray_clear(imeiarray) as imeiarray,

                  case
                    when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                      then imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                    else null
                  end as imei_arr,

                  case
                    when lower(trim(serialno)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or serialno is null then ''
                    when lower(trim(serialno)) rlike '^[0-9a-z]{6,32}$' then lower(trim(serialno))
                    else ''
                  end as serialno,

                  case
                    when lower(trim(adsid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or adsid is null then ''
                    when regexp_replace(lower(trim(adsid)), ' |-|\\.|:|\073','') rlike '0{32}' then ''
                    when regexp_replace(lower(trim(adsid)), ' |-|\\.|:|\073','') rlike '^[0-9a-f]{32}$'
                    then concat
                    (
                      substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 1, 8), '-',
                      substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 9, 4), '-',
                      substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 13, 4), '-',
                      substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 17, 4), '-',
                      substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 21, 12)
                    )
                    else ''
                  end as adsid,

                  case
                    when lower(trim(androidid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or androidid is null then ''
                    when lower(trim(androidid)) rlike '^[0-9a-f]{14,16}$' then lower(trim(androidid))
                    else ''
                  end as androidid,

                  case
                    when lower(trim(simserialno)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or simserialno is null then ''
                    when lower(trim(simserialno)) rlike '^[0-9a-z]{19,20}$' then lower(trim(simserialno))
                    else ''
                  end as simserialno,

                  case
                    when lower(trim(phoneno)) rlike '[0-9a-f]{32}' then ''
                    when length(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialn), trim(cast(carrier as string)),trim(imsi))), ',')[0]) = 17
                    then string_sub_str(split(extract_phone_num2(extract_phone_num3(trim(phoneno), trim(simserialn), trim(cast(carrier as string)), trim(imsi))), ',')[0])
                    else ''
                  end as phone

                  case
                    when lower(trim(imsi)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or imsi is null then ''
                    when trim(imsi) rlike '^[0-9]{14,15}$' then trim(imsi)
                    else ''
                  end as imsi,

                  cast(if(length(imsi)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as imsi_tm,
                  imsiarray_clear(imsiarray) as  imsiarray,

                  case
                    when get_mac(macarray) is not null and get_mac(macarray) <> '02:00:00:00:00:00'
                      then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
                    when `empty2null 'mac'` is not null
                      then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
                    else null
                  end as mac_tm,

                  cast(if(luhn_checker(regexp_replace(trim(imei), ' |/|-','')),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),null)  as string) as imei_tm,

                  case
                    when length(imei_array_union('field',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                      then imei_array_union('date',imeiarray_clear(imeiarray),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                    else null
                  end as imei_arr_tm,

                  cast(if(length(trim(serialno))=0 or serialno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as serialno_tm,
                  cast(if(length(trim(adsid))=0 or serialno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as adsid_tm,
                  cast(if(length(trim(androidid))=0 or serialno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as androidid_tm,
                  cast(if(length(trim(simserialno)) = 0 or simserialno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as simserialno_tm,
                  cast(if(length(trim(phoneno)) = 0 or phoneno is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as phoneno_tm
              from $log_device_info_jh as jh
              lateral view explode(coalesce(macarray, array(map()))) tf as m
              where jh.dt = '$day' and jh.plat = 1 and muid is not null and length(muid)=40
            ) device_info_jh
            left join
            (SELECT value FROM $blacklist where type='mac' and day=20180702 GROUP BY value) blacklist_mac
            on (substring(regexp_replace(regexp_replace(trim(lower(device_info_jh.mac)), ' |-|\\.|:|\073',''), '(.{2})', '\$1:'), 1, 17)=blacklist_mac.value)
        ) as a
        group by muid
    ) as b

    union all

    select
        muid,
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
            muid,

            case
              when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
              when regexp_replace(trim(lower(mac)), ' |-|\\.|:|\073', '') in ('000000000000', '020000000000') then ''
              when regexp_replace(trim(lower(mac)), ' |-|\\.|:|\073', '') rlike '^[0-9a-f]{12}$'
              then substring(regexp_replace(regexp_replace(trim(lower(mac)), ' |-|\\.|:|\073', ''), '(.{2})', '\$1:'), 1, 17)
              else ''
            end as mac,

            case
              when trim(udid) rlike '0{14,17}' then ''
              when length(trim(lower(udid))) = 16 and trim(udid) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(udid,1,14))), ' |/|-|imei:', '')), regexp_replace(trim(lower(udid)), ' |/|-|imei:', ''),'')
              when length(trim(lower(udid))) = 16 and trim(udid) not rlike '^[0-9]+$' then ''
              when imei_verify(regexp_replace(trim(lower(udid)), ' |/|-|udid:', '')) then regexp_replace(trim(lower(udid)), ' |/|-|imei:', '')
              else ''
            end as imei,

            case
              when lower(trim(adsid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or adsid is null then ''
              when regexp_replace(lower(trim(adsid)), ' |-|\\.|:|\073','') rlike '0{32}' then ''
              when regexp_replace(lower(trim(adsid)), ' |-|\\.|:|\073','') rlike '^[0-9a-f]{32}$'
              then concat
              (
                substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 1, 8), '-',
                substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 9, 4), '-',
                substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 13, 4), '-',
                substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 17, 4), '-',
                substring(regexp_replace(trim(upper(adsid)), ' |-|\\.|:|\073', '') , 21, 12)
              )
              else ''
            end as adsid,

            case
              when lower(trim(androidid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or androidid is null then ''
              when lower(trim(androidid)) rlike '^[0-9a-f]{14,16}$' then lower(trim(androidid))
              else ''
            end as androidid,

            case when length(mac)>0 then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as  string) else null end as mac_tm,
            cast(if(luhn_checker(regexp_replace(trim(udid), ' |/|-','')),unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'),null) as string)  as imei_tm,
            cast(if(length(adsid)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as adsid_tm,
            cast(if(length(androidid)=0,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as androidid_tm
        from $log_device_info as a
        left join (select muid  from $log_device_info_jh where dt = '$day' and plat = 1 and muid is not null  and length(muid)=40 group by muid) as jh
        on a.muid = jh.muid
        where jh.muid is null and a.plat = 1 and a.dt = '$day' and a.muid is not null and length(a.muid)=40
    ) as b
    group by muid
) as c
left join $dws_device_snsuid_list_android as e
on (case when length(c.muid)=40 then c.muid else concat('',rand()) end = e.deviceid)
) as f
) as g
where g.rank = 1

"
