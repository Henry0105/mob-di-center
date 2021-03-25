#!/bin/sh

day=$1

set -x -e

:<<!
imei-phone imeimd5-phone交换数据入库
md5加密的会去撞库
!
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
dataexchange_idmapping=${dw_ext_exchange}.dataexchange_idmapping
total_phone_md5_mapping=${dm_dataengine_mapping}.total_phone_md5_mapping
dm_imei_mapping_v2=${dm_dataengine_mapping}.dm_imei_mapping_v2
#mappping
#blacklist=dim_sdk_mapping.blacklist
#tmp
ext_phone_mapping_incr_pre=${dw_mobdi_tmp}.ext_phone_mapping_incr_pre
ext_phone_mapping_incr_phone_pre=${dw_mobdi_tmp}.ext_phone_mapping_incr_phone_pre
ext_phone_mapping_incr_phonemd5_pre=${dw_mobdi_tmp}.ext_phone_mapping_incr_phonemd5_pre
#output
#ext_phone_mapping_incr=dim_mobdi_mapping.ext_phone_mapping_incr

hive -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;


insert overwrite table $ext_phone_mapping_incr_pre
select
      owner_data,
      ext_data,
      source,
      plat,
      processtime,
      type
    from
    (
      select trim(param) as owner_data,
             trim(response) as ext_data ,
             vendor  as source,
             day as processtime,lower(trim(type)) as type,1 as plat
      from $dataexchange_idmapping
      where day = '$day'
   and lower(type) in
   ('imei_phone','imei_phonemd5','imeimd5_phone','imeimd5_phonemd5',
   'imei14_phone','imei14_phonemd5','imei14md5_phone','imei14md5_phonemd5',
   'imei15_phone','imei15_phonemd5','imei15md5_phone','imei15md5_phonemd5')
   and vendor is not null and (response rlike '^[1][3-9][0-9]{9}$|^([6|9])[0-9]{7}$|^[0][9][0-9]{8}$|^[6]([8|6])[0-9]{5}$' or length(response)=32)
    ) info
  where length(ext_data)>0;

insert overwrite table $ext_phone_mapping_incr_phone_pre
select owner_data,ext_data,source,plat,processtime,type
from
(
  select owner_data ,phone_md5_mapping.phone as ext_data, source,plat,processtime,type
  from
  (
   select * from $ext_phone_mapping_incr_pre where ext_data rlike '[0-9a-f]{32}'
  ) phone_md5
  left join
  $total_phone_md5_mapping phone_md5_mapping
  on phone_md5.ext_data=phone_md5_mapping.md5_phone
  where phone_md5_mapping.phone is not null
  union all
  select owner_data,ext_data,source,plat,processtime,type from $ext_phone_mapping_incr_pre where (not ext_data rlike '[0-9a-f]{32}')
)t
group by owner_data,ext_data,source,plat,processtime,type;

insert overwrite table $ext_phone_mapping_incr_phonemd5_pre
select owner_data,ext_data,source,plat,processtime,type
from
(
  select owner_data ,phone_md5.ext_data as ext_data, source,plat,processtime,type
  from
  (
   select * from $ext_phone_mapping_incr_pre where ext_data rlike '[0-9a-f]{32}'
  ) phone_md5
  left join
  $total_phone_md5_mapping phone_md5_mapping
  on phone_md5.ext_data=phone_md5_mapping.md5_phone
  where phone_md5_mapping.phone is null
)t
group by owner_data,ext_data,source,plat,processtime,type;


insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei_phone')
select owner_data,ext_data,source,plat,processtime
from
(
  select NVL(imei_md5_mapping.imei,imei_md5.owner_data) as owner_data, ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('imei_phone','imei_phonemd5','imeimd5_phone','imeimd5_phonemd5')
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.owner_data=imei_md5_mapping.imei_md5
  where (imei_md5_mapping.imei_md5 is not null or type in('imei_phone','imei_phonemd5'))
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imeimd5_phone')
select owner_data,ext_data,source,plat,processtime
from
(
  select imei_md5.owner_data as owner_data,  ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('imeimd5_phone','imeimd5_phonemd5')
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.owner_data=imei_md5_mapping.imei_md5
  where imei_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei14_phone')
select owner_data,ext_data,source,plat,processtime
from
(
  select NVL(imei14_md5_mapping.imei,imei14_md5.owner_data) as owner_data, ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('imei14_phone','imei14_phonemd5','imei14md5_phone','imei14md5_phonemd5')
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.owner_data=imei14_md5_mapping.imei_md5
  where (imei14_md5_mapping.imei_md5 is not null or type in('imei14_phone','imei14_phonemd5'))
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei14md5_phone')
select owner_data,ext_data,source,plat,processtime
from
(
  select imei14_md5.owner_data as owner_data, ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('imei14md5_phone','imei14md5_phonemd5')
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.owner_data=imei14_md5_mapping.imei_md5
  where imei14_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei15_phone')
select owner_data,ext_data,source,plat,processtime
from
(
  select NVL(imei15_md5_mapping.imei,imei15_md5.owner_data) as owner_data,  ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('imei15_phone','imei15_phonemd5','imei15md5_phone','imei15md5_phonemd5')
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.owner_data=imei15_md5_mapping.imei_md5
  where (imei15_md5_mapping.imei_md5 is not null or type in('imei15_phone','imei15_phonemd5'))
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei15md5_phone')
select owner_data,ext_data,source,plat,processtime
from
(
  select imei15_md5.owner_data as owner_data,  ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('imei15md5_phone','imei15md5_phonemd5')
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.owner_data=imei15_md5_mapping.imei_md5
  where imei15_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei_phonemd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select NVL(imei_md5_mapping.imei,imei_md5.owner_data) as owner_data,  ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type in ('imei_phonemd5','imeimd5_phonemd5')
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.owner_data=imei_md5_mapping.imei_md5
  where (imei_md5_mapping.imei_md5 is not null or type ='imei_phonemd5')
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imeimd5_phonemd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select imei_md5.owner_data as owner_data,  ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type ='imeimd5_phonemd5'
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.owner_data=imei_md5_mapping.imei_md5
  where imei_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei14_phonemd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select NVL(imei14_md5_mapping.imei,imei14_md5.owner_data) as owner_data, ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type in ('imei14_phonemd5','imei14md5_phonemd5')
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.owner_data=imei14_md5_mapping.imei_md5
  where (imei14_md5_mapping.imei_md5 is not null or type ='imei14_phonemd5')
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei14md5_phonemd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select imei14_md5.owner_data as owner_data, ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type ='imei14md5_phonemd5'
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.owner_data=imei14_md5_mapping.imei_md5
  where imei14_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei15_phonemd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select NVL(imei15_md5_mapping.imei,imei15_md5.owner_data) as owner_data,  ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type in ('imei15_phonemd5','imei15md5_phonemd5')
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.owner_data=imei15_md5_mapping.imei_md5
  where (imei15_md5_mapping.imei_md5 is not null or type ='imei15_phonemd5')
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imei15md5_phonemd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select imei15_md5.owner_data as owner_data,  ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type ='imei15md5_phonemd5'
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.owner_data=imei15_md5_mapping.imei_md5
  where imei15_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;
"

:<<!
phone-imei phonemd5-imei交换数据入库
md5加密的会去撞库
!

hive -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function IMEI_VERIFY AS 'com.youzu.mob.java.udf.ImeiVerify';

insert overwrite table $ext_phone_mapping_incr_pre
select
      owner_data,
      ext_data,
      source,
      plat,
      processtime,
   type
  from
  (
    select
       owner_data,
       CASE WHEN blacklist_imei.value IS NOT NULL THEN ''
          WHEN IMEI_VERIFY(info.ext_data) THEN SUBSTRING(regexp_replace(TRIM(LOWER(info.ext_data)), ' |/|-',''),1,15)
       ELSE '' END as ext_data,
       source,
       1 as plat,
       processtime,info.type
    from
    (
      select trim(param) as owner_data,
             trim(response) as ext_data ,
             vendor  as source,
             day as processtime,lower(trim(type)) as type
      from  $dataexchange_idmapping
      where day='$day'
   and lower(type) in
   ('phone_imei','phonemd5_imei','phone_imeimd5','phonemd5_imeimd5',
   'phone_imei14','phonemd5_imei14','phone_imei14md5','phonemd5_imei14md5',
   'phone_imei15','phonemd5_imei15','phone_imei15md5','phonemd5_imei15md5')
    and vendor is not null
    ) info
    LEFT JOIN
    (SELECT value FROM $blacklist where type='imei' and day='20180702' GROUP BY value) blacklist_imei
    on (case when info.ext_data is not null and length(info.ext_data) > 0 and lower(trim(info.ext_data)) != 'null' then regexp_replace(TRIM(LOWER(info.ext_data)), ' |/|-','') else 'llun' end=blacklist_imei.value)
  )tt
  where length(ext_data)>0;

insert overwrite table $ext_phone_mapping_incr_phone_pre
select owner_data,ext_data,source,plat,processtime,type
from
(
  select phone_md5_mapping.phone as owner_data, ext_data ,source,plat,processtime,type
  from
  (
   select * from $ext_phone_mapping_incr_pre where owner_data rlike '[0-9a-f]{32}'
  ) phone_md5
  left join
  $total_phone_md5_mapping phone_md5_mapping
  on phone_md5.owner_data=phone_md5_mapping.md5_phone
  where phone_md5_mapping.phone is not null
  union all
  select owner_data,ext_data,source,plat,processtime,type from $ext_phone_mapping_incr_pre where (not owner_data rlike '[0-9a-f]{32}')
)t
group by owner_data,ext_data,source,plat,processtime,type;

insert overwrite table $ext_phone_mapping_incr_phonemd5_pre
select owner_data,ext_data,source,plat,processtime,type
from
(
  select phone_md5.owner_data as owner_data, ext_data ,source,plat,processtime,type
  from
  (
   select * from $ext_phone_mapping_incr_pre where owner_data rlike '[0-9a-f]{32}'
  ) phone_md5
  left join
  $total_phone_md5_mapping phone_md5_mapping
  on phone_md5.owner_data=phone_md5_mapping.md5_phone
  where phone_md5_mapping.phone is null
)t
group by owner_data,ext_data,source,plat,processtime,type;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imei')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, NVL(imei_md5_mapping.imei,imei_md5.ext_data) as ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('phone_imei','phonemd5_imei','phone_imeimd5','phonemd5_imeimd5')
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.ext_data=imei_md5_mapping.imei_md5
  where (imei_md5_mapping.imei_md5 is not null or type in('phone_imei','phonemd5_imei'))
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imeimd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, imei_md5.ext_data as ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('phone_imeimd5','phonemd5_imeimd5')
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.ext_data=imei_md5_mapping.imei_md5
  where imei_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imei14')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, NVL(imei14_md5_mapping.imei,imei14_md5.ext_data) as ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('phone_imei14','phonemd5_imei14','phone_imei14md5','phonemd5_imei14md5')
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.ext_data=imei14_md5_mapping.imei_md5
  where (imei14_md5_mapping.imei_md5 is not null or type in('phone_imei14','phonemd5_imei14'))
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imei14md5')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, imei14_md5.ext_data as ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('phone_imei14md5','phonemd5_imei14md5')
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.ext_data=imei14_md5_mapping.imei_md5
  where imei14_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imei15')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, NVL(imei15_md5_mapping.imei,imei15_md5.ext_data) as ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('phone_imei15','phonemd5_imei15','phone_imei15md5','phonemd5_imei15md5')
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.ext_data=imei15_md5_mapping.imei_md5
  where (imei15_md5_mapping.imei_md5 is not null or type in('phone_imei15','phonemd5_imei15'))
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imei15md5')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, imei15_md5.ext_data as ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phone_pre where type in ('phone_imei15md5','phonemd5_imei15md5')
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.ext_data=imei15_md5_mapping.imei_md5
  where imei15_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phonemd5_imei')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, NVL(imei_md5_mapping.imei,imei_md5.ext_data) as ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type in ('phonemd5_imei','phonemd5_imeimd5')
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.ext_data=imei_md5_mapping.imei_md5
  where (imei_md5_mapping.imei_md5 is not null or type ='phonemd5_imei')
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phonemd5_imeimd5')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, imei_md5.ext_data as ext_data ,source,imei_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type ='phonemd5_imeimd5'
  ) imei_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei_md5_mapping
  on imei_md5.ext_data=imei_md5_mapping.imei_md5
  where imei_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phonemd5_imei14')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, NVL(imei14_md5_mapping.imei,imei14_md5.ext_data) as ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type in ('phonemd5_imei14','phonemd5_imei14md5')
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.ext_data=imei14_md5_mapping.imei_md5
  where (imei14_md5_mapping.imei_md5 is not null or type ='phonemd5_imei14')
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phonemd5_imei14md5')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, imei14_md5.ext_data as ext_data ,source,imei14_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type ='phonemd5_imei14md5'
  ) imei14_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei14_md5_mapping
  on imei14_md5.ext_data=imei14_md5_mapping.imei_md5
  where imei14_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phonemd5_imei15')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, NVL(imei15_md5_mapping.imei,imei15_md5.ext_data) as ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type in ('phonemd5_imei15','phonemd5_imei15md5')
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.ext_data=imei15_md5_mapping.imei_md5
  where (imei15_md5_mapping.imei_md5 is not null or type ='phonemd5_imei15')
)t
group by owner_data,ext_data,source,plat,processtime;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phonemd5_imei15md5')
select owner_data,ext_data,source,plat,processtime
from
(
  select owner_data, imei15_md5.ext_data as ext_data ,source,imei15_md5.plat,processtime
  from
  (
    select * from $ext_phone_mapping_incr_phonemd5_pre where type ='phonemd5_imei15md5'
  ) imei15_md5
  left join
  (select * from $dm_imei_mapping_v2 where day='$day') imei15_md5_mapping
  on imei15_md5.ext_data=imei15_md5_mapping.imei_md5
  where imei15_md5_mapping.imei_md5 is null
)t
group by owner_data,ext_data,source,plat,processtime;
"


:<<!
imsi-phone 交换数据入库
!

hive -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='imsi_phone')
select owner_data,ext_data,source,1 as plat,processtime
from
  (
    select trim(param) as owner_data,
           trim(response) as ext_data ,
           vendor  as source,
           day as processtime
    from $dataexchange_idmapping
    where day=$day
    and type ='imsi_phone'
    and vendor is not null
    and response rlike '^[1][3-8]\\\d{9}$|^([6|9])\\\d{7}$|^[0][9]\\\d{8}$|^[6]([8|6])\\\d{5}$'
)t
"


:<<!
phone-imsi 交换数据入库
!

hive -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

insert overwrite table $ext_phone_mapping_incr partition(day='$day',type='phone_imsi')
select owner_data,ext_data,source,plat,processtime
from
(
  select
      owner_data,
      CASE WHEN blacklist_imsi.value IS NOT NULL THEN ''
      WHEN TRIM(phone_imsi.ext_data) RLIKE '^[0-9]{14,15}$' THEN TRIM(phone_imsi.ext_data)
      ELSE ''
      END as ext_data,
      source,
      1 as plat,
      processtime
  from
    (
      select trim(param) as owner_data,
             trim(response) as ext_data ,
             vendor  as source,
             day as processtime
      from $dataexchange_idmapping
      where day=$day
      and type ='phone_imsi'
      and vendor is not null
  ) phone_imsi
  LEFT JOIN (SELECT value FROM $blacklist where type='imsi' and day=20180702 GROUP BY value) blacklist_imsi
  on (TRIM(phone_imsi.ext_data)=blacklist_imsi.value)
)t
where length(ext_data)>0
"