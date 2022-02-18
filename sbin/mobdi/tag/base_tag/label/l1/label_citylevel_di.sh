#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备的city_level和city_level_1001，模型计算需要这些特征
@projectName:MOBDI
'
# 只做为label_citylevel_full.sh使用, 没有model直接使用

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1

#input
#dws_device_ip_info="dm_mobdi_master.device_ip_info"
dws_device_ip_info=${dws_device_ip_info_di}
#mapping
area_mapping=$dim_mapping_area_par
city_level_mapping=$dim_mapping_city_level_par
#output
label_hometown_di=${label_l1_citylevel_di}

last_area_par=`hive -e "show partitions $area_mapping" | sort | tail -n 1`

#根据device_ip_info的表，计算国家省份城市,如果非3g/4g的，则取最新的一个非3g/4g所对应的记录,否则取3g/4g最新对应的
hive -v -e "
with device_countrys_label_incr as (
  select device,country,province,city
  from
  (
    select device,country,province,city,
           row_number() over( partition by device order by flag desc,timestamp desc) as rn
    from
    (
      select device,country,province,city,network,timestamp,
             case
               when instr(network,'3g')=0 and instr(network,'4g')=0 then 2
               else 1
             end as flag
      from $dws_device_ip_info
      where day='$day'
      and plat=1
      and network is not null
      and network!=''
    )tmp
  )un
  where rn=1
),
device_country_cn_label_incr as
(
  select tt.device as device,tt.country as country,tt.province as province,tt.city as city,
         country_area.country as country_cn,
         province_area.province as province_cn,
         city_area.city as city_cn,'$day' as day
  from device_countrys_label_incr tt
  left outer join
  (
    select country,country_code
    from $area_mapping
    where $last_area_par
    group by country,country_code
  ) country_area on tt.country=country_area.country_code
  left outer join
  (
    select province,province_code
    from $area_mapping
    where $last_area_par
    group by province,province_code
  ) province_area on tt.province=province_area.province_code
  left outer join
  (
    select city,city_code
    from $area_mapping
    where $last_area_par
    group by city,city_code
  ) city_area on tt.city=city_area.city_code
)
insert overwrite table $label_hometown_di partition(day='$day')
select tt.device as device,tt.country as country,tt.province as province,tt.city as city,tt.country_cn as country_cn,
       tt.province_cn as province_cn,tt.city_cn as city_cn,
       case
         when tt.country_cn='中国' and tt.province_cn not in('香港','澳门','台湾') and tt.city_cn<>'未知' and length(trim(tt.city_cn))>0 then coalesce(f.level,5)
         else -1
       end as city_level,
       case
         when tt.country_cn='中国' and tt.province_cn not in('香港','澳门','台湾') and tt.city_cn<>'未知' and length(trim(tt.city_cn))>0 then coalesce(g.level,6)
         else -1
       end as city_level_1001
from device_country_cn_label_incr tt
left outer join
(
  select *
  from $city_level_mapping
  where version='1000'
) f on tt.city_cn=f.city
left outer join
(
  select *
  from $city_level_mapping
  where version='1001'
) g on tt.city_cn=g.city;
"
