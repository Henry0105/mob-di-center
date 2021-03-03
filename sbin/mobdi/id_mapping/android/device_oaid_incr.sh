#!/bin/sh
set -e -x

day=$1

#output
outputTable="dm_mobdi_mapping.device_oaid_mapping_incr"

:<<!
create table if not exists dm_mobdi_mapping.device_oaid_mapping_incr
(
  `device` string COMMENT '设备id（可能是空）',
  `oaid` string COMMENT 'oaid',
  `oaid_tm` string COMMENT 'oaid最早上报时间',
  `duid` string COMMENT 'duid（只有awaken_dfl需要保留）')
COMMENT 'device与oaid的增量映射表，主键'
PARTITIONED BY (
  `day` string,
  `source` string)
stored as orc;
!

hive -v -e "
insert overwrite table $outputTable partition (day='$day',source='log_device_info_jh')
select trim(lower(device)) as device,
       oaid as oaid,
      unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as oaid_tm,
       '' as duid
from dw_mobdi_etl.log_device_info_jh
where dt='$day'
and oaid is not null
and lower(oaid) not in ('','null')
and lower(oaid)  rlike '^([A-Za-z0-9]|-)+$'
and plat=1
and trim(lower(device)) rlike '^[a-f0-9]{40}$'
and trim(device) != '0000000000000000000000000000000000000000'
;
"



hive -v -e "
insert overwrite table $outputTable partition (day='$day',source='awaken_dfl')

select b.device,a.oaid,a.oaid_tm,a.duid from
(select
         get_json_object(extra, '$.cnt_fids.fids.oaid') as oaid,
         substr(serdatetime,0,10) as oaid_tm,
         lower(trim(duid)) as duid
  from dw_sdk_log.awaken_dfl
  where day='$day'
  and get_json_object(extra, '$.cnt_fids.fids.oaid') is not null
  and lower(trim(get_json_object(extra, '$.cnt_fids.fids.oaid'))) not in ('','null')
  and lower(trim(get_json_object(extra, '$.cnt_fids.fids.oaid')))  rlike '^([A-Za-z0-9]|-)+$'
  and duid is not null
  and lower(trim(duid)) not in ('','null')
)a
left join
(
  select device,duid,processtime from
  (
    select trim(lower(device)) as device,
           trim(lower(duid)) as duid,
           processtime,
           row_number() over (partition by duid order by processtime desc) as rn
    from dm_sdk_mapping.device_duid_mapping_new
    where length(trim(device))>0
    and length(trim(duid))>0
    and trim(lower(device)) rlike '^[a-f0-9]{40}$'
    and trim(device) != '0000000000000000000000000000000000000000'
    and plat=1
  )m where rn=1
)b
on a.duid = b.duid
;

"
