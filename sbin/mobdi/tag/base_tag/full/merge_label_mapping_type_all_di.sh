#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 单纯的把一些mapping类的标签合并在一起
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

##input
#label_l1_vocation_special=$label_l1_vocation_special
#device_info_master_incr=$dwd_device_info_di
#label_l1_network_label_di=$label_l1_network_label_di
#label_l1_citylevel_di=$label_l1_citylevel_di

##output
device_mapping_label=$label_mapping_type_all_di

hive -v -e "
set mapred.max.split.size=125000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.auto.convert.join=true;
set hive.map.aggr=true;
insert overwrite table $device_mapping_label partition(day='$day')
select driver.device,
       case
         when info.carrier_clean='' then 'unknown'
         else info.carrier_clean
       end as carrier,
       case
         when info.factory_clean='' then 'unknown'
         else info.factory_clean
       end as cell_factory,
       case
         when info.sysver_clean='' then 'unknown'
         else info.sysver_clean
       end as sysver,
       case
         when info.model_clean='' then 'unknown'
         else info.model_clean
       end as model,
       case
         when info.price is null or info.price='' then -1
         when info.price > 2500 then 0
         when info.price>= 1000 and price <=2500 then 1
         when info.price>0 and info.price <1000 then 2
         ELSE - 1
       END AS model_level,
       case
         when info.screensize_clean='' then 'unknown'
         else info.screensize_clean
       end as screensize,
       case
         when info.breaked_clean is null then 'unknown'
         else lower(cast(info.breaked_clean as string))
       end as breaked,
       case
         when info.public_date='' then 'unknown'
         else info.public_date
       end as public_date,
       case
         when nw.network='' then 'unknown'
         else nw.network
       end as network,
       case
         when cn.country='' then 'unknown'
         else cn.country
       end as country,
       case
         when cn.province='' then 'unknown'
         else cn.province
       end as province,
       case
         when cn.city='' then 'unknown'
         else cn.city
       end as city,
       case
         when cn.country_cn='' then '未知'
         else cn.country_cn
       end as country_cn,
       case
         when cn.province_cn='' then '未知'
         else cn.province_cn
       end as province_cn,
       case
         when cn.city_cn='' then '未知'
         else cn.city_cn
       end as city_cn,
       cn.city_level,cn.city_level_1001,
       case
         when iden.identity is not null then cast(iden.identity as string)
         else iden.identity
       end as identity,
       case
         when info.price='' then 'unknown'
         else info.price
       end as price,
       case
         when info.factory_cn='' then 'unknown'
         else info.factory_cn
       end as factory_cn,
       case
         when info.factory_clean_subcompany='' then 'unknown'
         else info.factory_clean_subcompany
       end as factory_clean_subcompany,
       case
         when info.factory_cn_subcompany='' then 'unknown'
         else info.factory_cn_subcompany
       end as factory_cn_subcompany,
       case
         when info.sim_type='' then 'unknown'
         else info.sim_type
       end as sim_type,
       case
         when info.screen_size='' then 'unknown'
         else info.screen_size
       end as screen_size,
       case
         when info.cpu='' then 'unknown'
         else info.cpu
       end as cpu,
       case
         when info.sdcardstorage_clean='' then 'unknown'
         else info.sdcardstorage_clean
       end as sdcardstorage,
       case
         when info.ram_clean='' then 'unknown'
         else info.ram_clean
       end as ram,
       case
         when info.romimg_clean='' then 'unknown'
         else info.romimg_clean
       end as romimg,
       case
         when info.displayid_clean='' then 'unknown'
         else info.displayid_clean
       end as displayid
from
(
  select device
  from
  (
     select device
     from $label_l1_network_label_di
     where day='$day'

     union all

     select device
     from $label_l1_vocation_special
     where day='$day'

     union all

     select device
     from $dwd_device_info_di
     where day='$day'
     and plat=1
  )un
  group by device
)driver
left join
(
  select *
  from $dwd_device_info_di
  where day='$day'
  and plat=1
) info on driver.device=info.device
left join
(
  select *
  from $label_l1_network_label_di
  where day='$day'
) nw on driver.device=nw.device
left join
(
  select *
  from $label_l1_citylevel_di
  where day='$day'
)cn on driver.device=cn.device
left join
(
  select device ,identity
  from $label_l1_vocation_special
  where day=${day}
) iden on driver.device = iden.device
"

echo "handle mapping labels over of $day"
