#!/bin/sh

set -x -e

: '
@owner:hushk,luost
@describe:device_oaid_mapping_incr生成
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

#input
log_device_info_jh=dm_mobdi_master.dwd_log_device_info_jh_sec_di
awaken_dfl=dm_mobdi_master.dwd_awaken_dfl_sec_di
device_duid_mapping_new=dm_sdk_mapping.device_duid_mapping_new

#output
outputTable=dm_mobdi_mapping.device_oiid_mapping_sec_di

hive -v -e "
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $outputTable partition (day='$insert_day',source = 'log_device_info_jh')
select trim(lower(device)) as device,
       oiid as oiid,
       unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as oiid_tm,
       '' as duid
from $log_device_info_jh
where day = '$insert_day'
and oiid is not null
and lower(oiid) not in ('','null')
and plat=1
and trim(lower(device)) rlike '^[a-f0-9]{40}$'
and trim(device) != '0000000000000000000000000000000000000000';
"

:<<!
以dw_sdk_log.awaken_dfl表(oaid， duid)为基准，left join  dm_sdk_mapping.device_duid_mapping_new（device，duid） ,根据duid关联，得到device，oaid
!

hive -v -e "
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $outputTable partition (day='$insert_day',source='awaken_dfl')
select b.device,
       a.oiid,
       a.oiid_tm,
       a.duid
from
(
    select get_json_object(extra, '$.cnt_fids.fids.oiid') as oiid,
           substr(serdatetime,0,10) as oiid_tm,
           lower(trim(duid)) as duid
    from $awaken_dfl
    where day='$insert_day'
    and get_json_object(extra, '$.cnt_fids.fids.oiid') is not null
    and lower(trim(get_json_object(extra, '$.cnt_fids.fids.oiid'))) not in ('','null')
    and duid is not null
    and lower(trim(duid)) not in ('','null')
)a
left join
(
    select device,
           duid,
           processtime
    from
    (
        select trim(lower(device)) as device,
               trim(lower(duid)) as duid,
               processtime,
               row_number() over (partition by duid order by processtime desc) as rn
        from $device_duid_mapping_new
        where length(trim(device))>0
        and length(trim(duid))>0
        and trim(lower(device)) rlike '^[a-f0-9]{40}$'
        and trim(device) != '0000000000000000000000000000000000000000'
        and plat = 1
    )m where rn=1
)b
on a.duid = b.duid;
"
