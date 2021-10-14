#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

##input
#rp_device_location_permanent="rp_mobdi_app.rp_device_location_permanent"
#dim_mapping_area_par=dim_sdk_mapping.dim_mapping_area_par
#mapping_area_par="dm_sdk_mapping.mapping_area_par"
#dim_mapping_city_level_par=dim_sdk_mapping.dim_mapping_city_level_par
#dm_sdk_mapping.mapping_city_level_par

##output
#label_l2_permanent_place_mf="rp_mobdi_app.label_l2_permanent_place_mf"

area_mapping_lastpar=`hive -S -e "show partitions $dim_mapping_area_par" | tail -n 1`

date=`date -d "$day" +%d`
if [ $date -eq 01 ]
then
  hive -v -e "
  set hive.groupby.skewindata=true;
  set hive.exec.parallel=true;
  SET hive.merge.mapfiles=true;
  SET hive.merge.mapredfiles=true;
  set mapred.max.split.size=250000000;
  set mapred.min.split.size.per.node=128000000;
  set mapred.min.split.size.per.rack=128000000;
  set hive.merge.smallfiles.avgsize=250000000;
  set hive.merge.size.per.task = 250000000;


  insert overwrite table $label_l2_permanent_place_mf partition(day='$day')
  select permanent.device, permanent.country, permanent.province, permanent.city,
  coalesce(mapping_level.level,-1) as permanent_city_level,
  coalesce(country_mapping.country,'') as permanent_country_cn,
  coalesce(province_mapping.province,'') as permanent_province_cn,
  coalesce(city_mapping.city,'') as permanent_city_cn
  from
  (
    select device,country,province, city,day
    from $rp_device_location_permanent
    where day='$day'
  ) permanent
  left join
  (
    select area_mapping.city_code,city_level.level
    from
    (select city,city_code from $dim_mapping_area_par
     where $area_mapping_lastpar and city<>'' and city_code<>'' and country='中国' group by city,city_code) area_mapping
    left join
    (select city,level from $dim_mapping_city_level_par where version='1001' group by city,level) city_level
    on  area_mapping.city=city_level.city
  )mapping_level
  on permanent.city = mapping_level.city_code
  left join
  (select country,country_code from $dim_mapping_area_par where country_code<>'' and $area_mapping_lastpar group by country,country_code ) country_mapping
  on permanent.country=country_mapping.country_code
  left join
  (select province,province_code from $dim_mapping_area_par where province_code<>'' and $area_mapping_lastpar group by province,province_code ) province_mapping
  on permanent.province=province_mapping.province_code
  left join
  (select city,city_code from $dim_mapping_area_par where city_code<>'' and $area_mapping_lastpar group by city,city_code ) city_mapping
  on permanent.city=city_mapping.city_code;
  "
else
	echo $day
fi
