#!/bin/bash
: '
@owner: xiaolm
@describe:
@projectName:
@BusinessName:

'

set -x -e

if [ -z "$1" ]; then 
  exit 1
fi
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties



day=$1
prev_1day=`date +%Y%m%d -d "${day} -1 day"`

#input
#dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

#mapping
#dim_mapping_ios_factory_par=dim_sdk_mapping.dim_mapping_ios_factory_par
#dim_mapping_carrier_par=dim_sdk_mapping.dim_mapping_carrier_par


#out
#dwd_device_info_di=dm_mobdi_master.dwd_device_info_di
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df


ios_factory_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_mapping_ios_factory_par', 'version');
"
ios_factory_mapping_partition=(`hive -e "$ios_factory_mapping_sql"`)

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
  select device, factory, model, screensize, devicetype as devicetype, sysver, cast(breaked as string) as breaked, carrier, serdatetime, 1 as flag
  FROM $dwd_log_device_info_jh_sec_di
  where day = '$day'
  and plat = '2'
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

insert overwrite table $dwd_device_info_di partition(day='$day', plat='2')
select info.device,
       CASE
         WHEN info.factory IS not NULL and upper(trim(info.factory)) ='APPLE' THEN 'APPLE'
         WHEN length(info.factory)>0 and upper(trim(info.factory)) !='APPLE' THEN 'APPLE'
         ELSE ''
       end AS factory,
       info.model,
       info.screensize,
       info.public_date,
       info.model_type,
       info.sysver,
       info.breaked,
       info.carrier,  ---需要清洗
       info.price,
       info.devicetype,
       CASE
         WHEN info.factory IS not NULL and upper(trim(info.factory)) ='APPLE' THEN 'APPLE'
         ELSE ''
       end AS factory_clean,
       CASE
         WHEN lower(trim(info.model)) = 'null' OR info.model IS NULL OR trim(info.model) = '' OR trim(info.model) = '未知' OR upper(trim(info.model)) = 'UNKNOWN' OR upper(trim(info.model))='　　' THEN 'unknown'
         WHEN info.model_clean IS NULL THEN 'other'
         ELSE trim(upper(info.model_clean))
       END AS model_clean,
       CASE
         WHEN trim(info.screensize) = '0x0' OR lower(trim(info.screensize)) = 'null' OR info.screensize IS NULL OR trim(info.screensize) = '' OR trim(info.screensize) = '未知' THEN 'unknown'
         WHEN cast(split(info.screensize, 'x') [0] AS INT) >= cast(split(info.screensize, 'x') [1] AS INT) THEN CONCAT (split(info.screensize, 'x') [1], 'x', split(info.screensize, 'x') [0])
         WHEN cast(split(info.screensize, 'x') [0] AS INT) < cast(split(info.screensize, 'x') [1] AS INT) THEN CONCAT (split(info.screensize, 'x') [0], 'x', split(info.screensize, 'x') [1])
         ELSE 'other'
       END AS screensize_clean,
       CASE
         WHEN info.devicetype is null or trim(info.devicetype) = '' THEN ''
         ELSE upper(trim(info.devicetype))
       END AS devicetype_clean,
       CASE
         WHEN lower(trim(info.sysver)) != 'null' and info.sysver IS not NULL and info.sysver rlike '^[a-zA-Z0-9,. ]*$' THEN info.sysver
         ELSE ''
       END AS sysver_clean,
       CASE
         WHEN lower(trim(info.breaked)) = 'false' THEN false
         WHEN lower(trim(info.breaked)) = 'true' THEN true
         ELSE NULL
       end as breaked_clean,
       CASE
         WHEN info.carrier = -1 OR info.carrier ='' OR info.carrier IS NULL THEN 'other'
         when trim(upper(coalesce(carrier_mapping.brand, info.carrier)))=-1 OR trim(upper(coalesce(carrier_mapping.brand, info.carrier)))='' THEN 'unknown'
         ELSE trim(upper(coalesce(carrier_mapping.brand, info.carrier)))
       END AS carrier_clean,
       '','','','','','','','','','','','','',''
from
(
  select ranked_device_info.device,
         ranked_device_info.factory,
         ranked_device_info.model,
         ranked_device_info.screensize,
         iosm.public_date,
         '' model_type,
         ranked_device_info.sysver,
         ranked_device_info.breaked,
         ranked_device_info.carrier,
         case when iosm.price ='' or iosm.price is null then 'unknown' else iosm.price end as price,
         devicetype,
		 iosm.model as model_clean
  from ranked_device_info
  left join
  (
    select *
    from $dim_mapping_ios_factory_par
    where version = '$ios_factory_mapping_partition'
  ) iosm on upper(iosm.model_code) = upper(ranked_device_info.model)
) info
LEFT JOIN
(select mcc_mnc,brand from $dim_mapping_carrier_par where version='1000') carrier_mapping
ON info.carrier = carrier_mapping.mcc_mnc
;

--插入全量表
insert overwrite table $dwd_device_info_df partition(version='${day}.1000', plat='2')
select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime,model_origin,
'','','','','','','',sysver_origin,carrier_origin,'','','','','','','',''
from 
(
  select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime,model_origin,sysver_origin,carrier_origin,
         row_number() over(partition by device order by processtime desc) as rank
  from 
  (
    select device, factory, model_clean as model, screensize_clean as screensize, public_date, model_type, sysver_clean as sysver,
           breaked_clean as breaked, carrier_clean as carrier, price, devicetype_clean as devicetype, day as processtime, model as model_origin,sysver as sysver_origin,carrier as carrier_origin
    from $dwd_device_info_di
    where day ='$day'
    and plat='2'

    union all

    select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime ,model_origin,sysver_origin,carrier_origin
    from $dwd_device_info_df
    where version = '${prev_1day}.1000'
    and plat='2'
  ) unioned
) ranked
where rank = 1
;
"
