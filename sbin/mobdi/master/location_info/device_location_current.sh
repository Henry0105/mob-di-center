#!/bin/bash

set -e -x

: '
@owner:zhoup
@describe:汇总设备当天出现过的国家省市,为常住地准准备
@projectName:mobdi
'

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

#入参
insert_day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#input
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di

#output
#dws_device_location_current_di=dm_mobdi_topic.dws_device_location_current_di

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;
SET hive.merge.smallfiles.avgsize=256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;

INSERT OVERWRITE TABLE $dws_device_location_current_di partition (day='$insert_day')
SELECT device,
       collect_set(map('country', country, 'province', province, 'city', city, 'type', type_str)) AS location,
       plat
FROM
(
    SELECT device,
           country,
           province,
           city,
           concat_ws(',', collect_set(type)) as type_str,
           plat
    FROM
    (
        SELECT device,
               CASE WHEN lower(country) = 'unknown' OR length(trim(country)) = 0 OR country IS NULL THEN '' ELSE country END AS country,
               CASE WHEN lower(province) = 'unknown' OR length(trim(province)) = 0 OR province IS NULL THEN '' ELSE province END AS province,
               CASE WHEN lower(city) = 'unknown' OR length(trim(city)) = 0 OR city IS NULL THEN '' ELSE city END AS city,
               'ip' as type,
               plat
        FROM $dws_device_ip_info_di
        WHERE day = '$insert_day'

        UNION ALL

        SELECT device,
               CASE WHEN lower(country) = 'unknown' OR length(trim(country)) = 0 OR country IS NULL THEN '' ELSE country END AS country,
               CASE WHEN lower(province) = 'unknown' OR length(trim(province)) = 0 OR province IS NULL THEN '' ELSE province END AS province,
               CASE WHEN lower(city) = 'unknown' OR length(trim(city)) = 0 OR city IS NULL THEN '' ELSE city END AS city,
               type,
               plat
        FROM $dws_device_location_staying_di
        WHERE day = '$insert_day'
        AND type <> 'ip'
    ) unioned
    group by device, country, province, city, plat
) grouped
WHERE (length(country) > 0 OR length(province) > 0 OR length(city) > 0)
GROUP BY device, plat;
"
