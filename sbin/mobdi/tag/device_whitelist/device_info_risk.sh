#!/bin/sh

day=$1
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df
#dim_brand_model_mapping_par=dim_sdk_mapping.dim_brand_model_mapping_par
#tmp
device_full_with_brand_mapping=${dm_mobdi_tmp}.device_full_with_brand_mapping
#out
device_info_risk=${dm_mobdi_tmp}.device_info_risk


hive -e"
insert overwrite table $device_full_with_brand_mapping 
 select device_info_master.device, device_info_master.factory, device_info_master.model, device_info_master.screensize, device_info_master.public_date, device_info_master.model_type, device_info_master.sysver, device_info_master.breaked, device_info_master.carrier, device_info_master.price, device_info_master.devicetype, device_info_master.processtime,brand_model_mapping.factory as mapping_factory,brand_model_mapping.model as mapping_model
  from 
  (
    select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime
    from $dwd_device_info_df
    where version = '${day}.1000' and plat = '1'
  ) device_info_master
  left join 
  (
    select upper(trim(brand)) as factory, upper(trim(model)) as model
    from $dim_brand_model_mapping_par
    where version='1000'
    group by upper(trim(brand)), upper(trim(model))
  ) brand_model_mapping
  on upper(trim(device_info_master.factory)) = brand_model_mapping.factory and upper(trim(device_info_master.model)) = brand_model_mapping.model
"

  
hive -e"
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with device_full_not_in_brand_mapping as (
  select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime
  from $device_full_with_brand_mapping
  where mapping_factory is null and mapping_model is null
),
brand_not_in_factory_mapping as(
  select device_full_not_in_brand_mapping.*
  from device_full_not_in_brand_mapping
  left join 
  (
    select upper(trim(brand)) as factory
    from $dim_brand_model_mapping_par
    where version='1000'
    group by upper(trim(brand))
  ) brand_model_mapping 
  on upper(trim(device_full_not_in_brand_mapping.factory)) = brand_model_mapping.factory
  where brand_model_mapping.factory is not null
),
factory_in_mapping as(
  select model_split.*, brand_model_mapping_fatory.factory as factory_real, brand_model_mapping_brand.factory as factory_from_model
  from 
  (
    select *, split(regexp_replace(trim(model), '-', ' '), ' ')[0] as brand_maybe
    from 
    (
      select *
      from brand_not_in_factory_mapping
      where trim(upper(model)) not in ('', 'UNKNOWN')
    ) a 
  ) as model_split
  left join 
  (
    select upper(trim(brand)) as factory
    from $dim_brand_model_mapping_par
    where version='1000'
    group by upper(trim(brand))
  ) as brand_model_mapping_fatory
  on upper(trim(model_split.factory)) = brand_model_mapping_fatory.factory
  left join 
  (
    select upper(trim(brand)) as factory
    from $dim_brand_model_mapping_par
    where version='1000'
    group by upper(trim(brand))
  ) as brand_model_mapping_brand 
  on upper(trim(model_split.brand_maybe)) = brand_model_mapping_brand.factory
),
refine_factory as(
  select *, 
  case 
    when (factory_real = 'BBK' and factory_from_model = 'VIVO') or 
         (factory_real = 'BBK' and factory_from_model = 'OPPO') or 
         (factory_real = 'BLEPHONE' and factory_from_model = 'LEPHONE') or 
         (factory_real = 'COOLPAD' and factory_from_model = 'YULONG') or 
         (factory_real = 'GUANGXIN' and factory_from_model = 'KINGSUN') or 
         (factory_real = 'HMD GLOBAL' and factory_from_model = 'NOKIA') or 
         (factory_real = 'HONOR' and factory_from_model = 'HUAWEI') or 
         (factory_real = 'HUAWEI' and factory_from_model = 'HONOR') or 
         (factory_real = 'INFINIX MOBILITY' and factory_from_model = 'INFINIX') or 
         (factory_real = 'ITEL MOBILE LIMITED' and factory_from_model = 'ITEL') or 
         (factory_real = 'JINKUPO' and factory_from_model = 'JKOPO') or 
         (factory_real = 'JINKUPO' and factory_from_model = 'KOPO') or 
         (factory_real = 'LEMOBILE' and factory_from_model = 'LETV') or 
         (factory_real = 'LOVME-T19' and factory_from_model = 'LOVME') or 
         (factory_real = 'TECNO MOBILE LIMITED' and factory_from_model = 'TECNO') or 
         (factory_real = 'YULONG' and factory_from_model = 'COOLPAD') then 1
  else 0
  end as refine_factory
  from factory_in_mapping
  where factory_from_model is not null and factory_real <> factory_from_model
),
brand_not_in_mapping_factory as(
  select device_full_not_in_brand_mapping.*
  from device_full_not_in_brand_mapping
  left join 
  (
    select upper(trim(brand)) as factory
    from $dim_brand_model_mapping_par
    where version='1000'
    group by upper(trim(brand))
  ) as brand_model_mapping 
  on upper(trim(device_full_not_in_brand_mapping.factory)) = brand_model_mapping.factory
  where brand_model_mapping.factory is null
),
brand_not_unknown as(
  select *
  from brand_not_in_mapping_factory
  where trim(upper(model)) not in ('', 'UNKNOWN') and trim(upper(factory)) not in ('', 'UNKNOWN') and factory is not null and model is not null
),
not_unknown_have_chinese as (
  select *, 
  case 
    when factory rlike '爱我|邦华|波导|博瑞|传奇数码|锤子|朵唯|格力|海尔|海信|华硕|华为|金立|康佳|酷比|酷派|乐视|联想|美图|魅族|摩托罗拉|纽曼|诺基亚|诺亚信|青橙|三星|索尼|糖果|天语|维图|夏普|小辣椒|小米|一加|长虹|中兴' 
    or model rlike '爱我|邦华|波导|博瑞|传奇数码|锤子|朵唯|格力|海尔|海信|华硕|华为|金立|康佳|酷比|酷派|乐视|联想|美图|魅族|摩托罗拉|纽曼|诺基亚|诺亚信|青橙|三星|索尼|糖果|天语|维图|夏普|小辣椒|小米|一加|长虹|中兴'
  then 1
  else 0
  end as chinese_big_factory
  from brand_not_unknown
  where factory rlike '[\\\\u4e00-\\\\u9fa5]' or model rlike '[\\\\u4e00-\\\\u9fa5]'
),
not_unknown_is_strange_factory as (
  select *, 
  case 
    when trim(upper(factory)) in ('MICROMAX', 'INTEX', 'QMOBILE', 'WIKO', 'FREEME', 'I-MOBILE', 'SONY ERICSSON', 'QIKU', 'LEECO', 'ACER', 'PANTECH', 'ETON', 'SKYWORTH', 'KARBONN', 'AMAZON', 'SKYHON', 'MYPHONE', 'MOBIISTAR', 'OPSSON', 'PANASONIC', 'AUX', 'LG', 'XOLO', 'LYF', 'AMOI', 'MI', 'KYOCERA', 'YEPEN', 'PRESTIGIO', 'INFOCUS') then 1
  else 0
  end as if_strange_factory
  from brand_not_unknown
  where factory not rlike '[\\\\u4e00-\\\\u9fa5]' and model not rlike '[\\\\u4e00-\\\\u9fa5]'
),
brand_is_unknown as(
  select *
  from brand_not_in_mapping_factory
  where (trim(upper(model)) in ('', 'UNKNOWN') or model is null) and trim(upper(factory)) not in ('', 'UNKNOWN') and factory is not null
),
unknown_have_chinese as(
  select *, 
  case 
    when factory rlike '爱我|邦华|波导|博瑞|传奇数码|锤子|朵唯|格力|海尔|海信|华硕|华为|金立|康佳|酷比|酷派|乐视|联想|美图|魅族|摩托罗拉|纽曼|诺基亚|诺亚信|青橙|三星|索尼|糖果|天语|维图|夏普|小辣椒|小米|一加|长虹|中兴' 
    or model rlike '爱我|邦华|波导|博瑞|传奇数码|锤子|朵唯|格力|海尔|海信|华硕|华为|金立|康佳|酷比|酷派|乐视|联想|美图|魅族|摩托罗拉|纽曼|诺基亚|诺亚信|青橙|三星|索尼|糖果|天语|维图|夏普|小辣椒|小米|一加|长虹|中兴'
  then 1
  else 0
  end as chinese_big_factory
  from brand_is_unknown
  where factory rlike '[\\\\u4e00-\\\\u9fa5]'
),
unknown_is_strange_factory as (
  select *, 
  case 
    when trim(upper(factory)) in ('MICROMAX', 'INTEX', 'QMOBILE', 'WIKO', 'FREEME', 'I-MOBILE', 'SONY ERICSSON', 'QIKU', 'LEECO', 'ACER', 'PANTECH', 'ETON', 'SKYWORTH', 'KARBONN', 'AMAZON', 'SKYHON', 'MYPHONE', 'MOBIISTAR', 'OPSSON', 'PANASONIC', 'AUX', 'LG', 'XOLO', 'LYF', 'AMOI', 'MI', 'KYOCERA', 'YEPEN', 'PRESTIGIO', 'INFOCUS') then 1
  else 0
  end as if_strange_factory
  from brand_is_unknown
  where factory not rlike '[\\\\u4e00-\\\\u9fa5]'
),
factory_is_unknown_brand_is_not_unknown as(
  select *
  from brand_not_in_mapping_factory
  where trim(upper(model)) not in ('', 'UNKNOWN') and (trim(upper(factory)) in ('', 'UNKNOWN') or factory is null)
),
factoryisunknown_brandisnotunknown_havechinese as(
  select *, 
  case 
    when model rlike '爱我|邦华|波导|博瑞|传奇数码|锤子|朵唯|格力|海尔|海信|华硕|华为|金立|康佳|酷比|酷派|乐视|联想|美图|魅族|摩托罗拉|纽曼|诺基亚|诺亚信|青橙|三星|索尼|糖果|天语|维图|夏普|小辣椒|小米|一加|长虹|中兴'
  then 1
  else 0
  end as chinese_big_factory
  from factory_is_unknown_brand_is_not_unknown
  where model rlike '[\\\\u4e00-\\\\u9fa5]'
)

insert overwrite table $device_info_risk
select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0 as risk, 1 as flag
from $device_full_with_brand_mapping
where mapping_factory is not null and mapping_model is not null

union all 

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.2 as risk, 2 as flag
from 
(
  select *
  from brand_not_in_factory_mapping
  where trim(upper(model)) in ('', 'UNKNOWN')
)t1

union all 

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.1 as risk, 3 as flag
from factory_in_mapping
where factory_from_model is null or factory_real = factory_from_model

union all 

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 1 as risk, 4 as flag
from refine_factory
where refine_factory = 0

union all 

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.2 as risk, 5 as flag
from refine_factory
where refine_factory = 1

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.7 as risk, 6 as flag
from not_unknown_have_chinese
where chinese_big_factory = 1

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.8 as risk, 7 as flag
from not_unknown_have_chinese
where chinese_big_factory = 0

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.4 as risk, 8 as flag
from not_unknown_is_strange_factory
where if_strange_factory = 1

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.7 as risk, 9 as flag
from not_unknown_is_strange_factory
where if_strange_factory = 0

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.8 as risk, 10 as flag
from unknown_have_chinese
where chinese_big_factory = 1

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.9 as risk, 11 as flag
from unknown_have_chinese
where chinese_big_factory = 0

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.5 as risk, 12 as flag
from unknown_is_strange_factory
where if_strange_factory = 1

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.8 as risk, 13 as flag
from unknown_is_strange_factory
where if_strange_factory = 0

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.7 as risk, 14 as flag
from 
(
  select *
  from brand_not_in_mapping_factory
  where (trim(upper(model)) in ('', 'UNKNOWN') or model is null) and (trim(upper(factory)) in ('', 'UNKNOWN') or factory is null)
)t2

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.6 as risk, 15 as flag
from factory_is_unknown_brand_is_not_unknown
where model not rlike '[\\\\u4e00-\\\\u9fa5]'

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.8 as risk, 16 as flag
from factoryisunknown_brandisnotunknown_havechinese
where chinese_big_factory = 1

union all

select device, factory, model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, 0.9 as risk, 17 as flag
from factoryisunknown_brandisnotunknown_havechinese
where chinese_big_factory = 0
"