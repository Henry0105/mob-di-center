#!/bin/bash
: '
@owner: haom
@describe:
@projectName:
@BusinessName:
@TableRelation:
'

set -x -e

if [ -z "$1" ]; then
  exit 1
fi
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties




#input
#dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

#mapping
#sysver_mapping_par=dm_sdk_mapping.sysver_mapping_par
#brand_model_mapping_par=dm_sdk_mapping.brand_model_mapping_par
#mapping_carrier_par=dm_mobdi_mapping.mapping_carrier_par

#out
#dwd_device_info_di=dm_mobdi_master.dwd_device_info_di
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df


day=$1
prev_1day=`date +%Y%m%d -d "${day} -1 day"`

hive -v -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.optimize.skewjoin = true;
SET hive.exec.reducers.bytes.per.reducer = 256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.exec.reducers.max=4000;

with unioned_device_info as (
  select muid as device, factory, model, screensize, devicetype as devicetype, sysver, cast(breaked as string) as breaked, carrier, serdatetime, 1 as flag
  FROM $dwd_log_device_info_jh_sec_di
  where day = '$day'
  and plat = '1'
),

ranked_device_info as (
  select device, factory, model, screensize, devicetype, sysver, breaked, carrier
  from
  (
    select device, factory, model, screensize, devicetype, sysver, breaked, carrier,
           row_number() over(partition by device order by flag desc, serdatetime desc) as rank
    FROM unioned_device_info
    where device is not null
    and length(device)= 40
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  ) ranked
  where rank = 1
)

insert overwrite table $dwd_device_info_di partition(day='$day', plat='1')
select info.device,
       info.factory,
       info.model,
       info.screensize,
       info.public_date,
       info.model_type,
       info.sysver,
       info.breaked,
       info.carrier,
       info.price,
       info.devicetype,
       CASE
         WHEN lower(trim(info.factory)) in ('null','none','na','other') OR info.factory IS NULL OR trim(info.factory) = '' OR trim(info.factory) = '未知' OR trim(upper(info.factory))='UNKNOWN' THEN 'unknown'
         ELSE coalesce(upper(trim(info.brand_mapping_brand)), 'other')
       end AS factory_clean,
       CASE
         WHEN lower(trim(info.model)) = 'null' OR info.model IS NULL OR trim(info.model) = '' OR trim(info.model) = '未知' OR upper(trim(info.model)) = 'UNKNOWN' OR upper(trim(info.model))='　　' THEN 'unknown'
         WHEN brand_mapping_clean_model IS NULL THEN 'other'
         ELSE trim(upper(info.brand_mapping_clean_model))
       END AS model_clean,
       CASE
         WHEN trim(info.screensize) = '0x0' OR lower(trim(info.screensize)) = 'null' OR info.screensize IS NULL OR trim(info.screensize) = '' OR trim(info.screensize) = '未知' THEN 'unknown'
         WHEN cast(split(info.screensize, 'x') [0] AS INT) >= cast(split(info.screensize, 'x') [1] AS INT) THEN CONCAT (split(info.screensize, 'x') [1], 'x', split(info.screensize, 'x') [0])
         WHEN cast(split(info.screensize, 'x') [0] AS INT) < cast(split(info.screensize, 'x') [1] AS INT) THEN CONCAT (split(info.screensize, 'x') [0] ,'x', split(info.screensize, 'x') [1])
         ELSE 'other'
       END AS screensize_clean,
       CASE
         WHEN info.devicetype is null or trim(info.devicetype) = '' THEN ''
         ELSE upper(trim(info.devicetype))
       END AS devicetype_clean,
       CASE
         WHEN lower(trim(info.sysver)) = 'null' OR info.sysver IS NULL OR trim(info.sysver) = '' OR trim(info.sysver) = '未知' THEN 'unknown'
         WHEN info.sysver = sysver_mapping.vernum THEN substr(sysver_mapping.sysver, 1, 3)
         WHEN substring(regexp_replace(info.sysver, 'Android|[.]|[ ]', ''), 1, 2) in ('11','15','16','20','21','22','23','24','30','31','32','40','41','42','43','44','50','51','60','70','71','80','81','90')
           THEN CONCAT (substring(regexp_replace(info.sysver, 'Android|[.]|[ ]', ''), 1, 1), '.', substring(regexp_replace(info.sysver, 'Android|[.]|[ ]', ''), 2, 1))
         ELSE 'unknown'
       END AS sysver_clean,
       CASE
         WHEN lower(trim(info.breaked)) = 'false' THEN false
         WHEN lower(trim(info.breaked)) = 'true' THEN true
         ELSE NULL
       end as breaked_clean,
       CASE
         WHEN info.carrier = -1 OR info.carrier ='' or info.carrier is null THEN 'other'
         when trim(upper(coalesce(carrier_mapping.brand, info.carrier)))=-1 OR trim(upper(coalesce(carrier_mapping.brand, info.carrier)))='' THEN 'unknown'
         ELSE trim(upper(coalesce(carrier_mapping.brand, info.carrier)))
       END AS carrier_clean,
       CASE
         WHEN lower(trim(info.factory)) in ('null','none','na','other') OR info.factory IS NULL OR trim(info.factory) = '' OR trim(info.factory) = '未知' OR trim(upper(info.factory))='UNKNOWN' THEN 'unknown'
         ELSE coalesce(upper(trim(info.brand_cn)), 'other')
       end AS factory_cn,
       CASE
         WHEN lower(trim(info.factory)) in ('null','none','na','other') OR info.factory IS NULL OR trim(info.factory) = '' OR trim(info.factory) = '未知' OR trim(upper(info.factory))='UNKNOWN' THEN 'unknown'
         ELSE coalesce(upper(trim(info.clean_brand_origin)), 'other')
       end AS factory_clean_subcompany,
       CASE
         WHEN lower(trim(info.factory)) in ('null','none','na','other') OR info.factory IS NULL OR trim(info.factory) = '' OR trim(info.factory) = '未知' OR trim(upper(info.factory))='UNKNOWN' THEN 'unknown'
         ELSE coalesce(upper(trim(info.brand_cn_origin)), 'other')
       end AS factory_cn_subcompany,
       coalesce(info.sim_type, '') as sim_type,
       coalesce(info.screen_size, '') as screen_size,
       coalesce(info.cpu, '') as cpu
FROM
(
  select ranked_device_info.device,
         ranked_device_info.factory,
         ranked_device_info.model,
         ranked_device_info.screensize,
         brand_mapping.public_date as public_date,
         brand_mapping.type as model_type,
         ranked_device_info.sysver,
         ranked_device_info.breaked,
         ranked_device_info.carrier,
         brand_mapping.price as price,devicetype,
         brand_mapping.model as brand_mapping_model,
         brand_mapping.clean_brand as brand_mapping_brand,
         brand_mapping.clean_model as brand_mapping_clean_model,
         brand_mapping.brand_cn as brand_cn,
         brand_mapping.clean_brand_origin as clean_brand_origin,
         brand_mapping.brand_cn_origin as brand_cn_origin,
         brand_mapping.sim_type as sim_type,
         brand_mapping.screen_size as screen_size,
         brand_mapping.cpu as cpu
  FROM ranked_device_info
  LEFT JOIN
  (
    select
    brand, model, public_time_clean as public_date, price, clean_brand, clean_model, brand_cn, clean_brand_origin, brand_cn_origin, sim_type, screen_size, cpu,
    case
      when price < 1000 then '低端'
      when price >= 1000 and price<=2500 then '中端'
      when price > 2500 then '高端'
    end as type
    from $brand_model_mapping_par
    where version='1000'
  ) brand_mapping
  on upper(trim(brand_mapping.brand)) = upper(trim(ranked_device_info.factory))
  and upper(trim(brand_mapping.model)) = upper(trim(ranked_device_info.model))
) info
LEFT JOIN
(select * from  $sysver_mapping_par  where version='1003')sysver_mapping
ON info.sysver = sysver_mapping.vernum
LEFT JOIN
(select mcc_mnc,brand from $mapping_carrier_par where version='1000') carrier_mapping
ON info.carrier = carrier_mapping.mcc_mnc
;

--插入全量表
insert overwrite table $dwd_device_info_full partition(version='${day}.1000', plat='1')
select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime,model_origin,
factory_clean, factory_cn, factory_clean_subcompany, factory_cn_subcompany, sim_type, screen_size, cpu
from
(
  select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime,model_origin,
  factory_clean, factory_cn, factory_clean_subcompany, factory_cn_subcompany, sim_type, screen_size, cpu,
  row_number() over(partition by device order by processtime desc) as rank
  from
  (
    select device, factory, model_clean as model, screensize_clean as screensize, public_date, model_type, sysver_clean as sysver,
           breaked_clean as breaked, carrier_clean as carrier, price, devicetype_clean as devicetype, day as processtime,model as model_origin,
           factory_clean, factory_cn, factory_clean_subcompany, factory_cn_subcompany, sim_type, screen_size, cpu
    from $dwd_device_info_di
    where day='$day'
    and plat='1'

    union all

    select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime,model_origin,
    factory_clean, factory_cn, factory_clean_subcompany, factory_cn_subcompany, sim_type, screen_size, cpu
    from $dwd_device_info_full
    where version='${prev_1day}.1000'
    and plat='1'
  ) unioned
) ranked
where rank = 1
;
"
