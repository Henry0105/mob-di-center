#!/bin/bash

: '
@owner:haom
@describe:设备日活表汇总,也可以作为设备每天的ip地址汇总
@projectName:mobdi
'

set -x -e

if [ -z "$1" ]; then 
  exit 1
fi

#入参
insert_day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#input
#dwd_base_station_info_sec_di=dm_mobdi_master.dwd_base_station_info_sec_di
#dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di
#dwd_auto_location_info_sec_di=dm_mobdi_master.dwd_auto_location_info_sec_di
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di
#dwd_t_location_sec_di=dm_mobdi_master.dwd_t_location_sec_di

#mapping
#mapping_ip_attribute_code=dm_sdk_mapping.mapping_ip_attribute_code

#out
#device_ip_info=dm_mobdi_topic.dws_device_ip_info_di


#取dm_sdk_mapping.mapping_ip_attribute_code最新分区
ip_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_sdk_mapping', 'mapping_ip_attribute_code', 'day');
"
last_ip_mapping_partition=(`hive -e "$ip_mapping_sql"`)

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true; 
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx4096m';
SET mapreduce.child.map.java.opts='-Xmx4096m';
SET hive.optimize.skewjoin=true;
SET hive.exec.reducers.bytes.per.reducer = 1000000000;
set mapreduce.reduce.memory.mb=6144;

ALTER TABLE $dws_device_ip_info_di DROP IF EXISTS PARTITION (day='$insert_day');
insert overwrite table $dws_device_ip_info_di partition(day='$insert_day')
select device,
       plat,
       ipaddr,
       timestamp,
       network,
       language,
       nvl(ip_mapping.country_code, '') as country,
       nvl(ip_mapping.province_code,'') as province,
       nvl(ip_mapping.city_code,'') as city,
       ip_mapping.area_code as area
from 
(
    select device,
           plat,
           ipaddr,
           timestamp,
           concat_ws(',', collect_set(case when network='' then null else network end)) as network,
           concat_ws(',', collect_set(case when language='' then null else language end)) as language
    from
    (
        select device,
               plat,
               ipaddr,
               timestamp,
               if(network is null or (trim(lower(network)) not rlike '^(2g)|(3g)|(4g)|(5g)|(cell)|(wifi)|(bluetooth)$'),'',trim(lower(network))) as network,
               if(language is null or trim(lower(language)) in ('null','unknown'),'',trim(language)) as language
        from
        (
            select muid as device,
                   plat,
                   ipaddr,
                   CONCAT(unix_timestamp(serdatetime),'000') as timestamp,
                   networktype as network,
                   language
            from $dwd_base_station_info_sec_di
            where day='${insert_day}'

            union all

            select muid as device,
                   plat,
                   clientip as ipaddr,
                   serdatetime as timestamp,
                   networktype as network,
                   language
            from $dwd_location_info_sec_di
            where day='${insert_day}'

            union all

            select muid as device,
                   plat,
                   clientip as ipaddr,
                   serdatetime as timestamp,
                   networktype as network,
                   language
            from $dwd_auto_location_info_sec_di
            where day='${insert_day}'

            union all

            select muid as device,
                   plat,
                   ipaddr,
                   CONCAT(unix_timestamp(serdatetime),'000') as timestamp,
                   networktype as network,
                   language
            from $dwd_log_wifi_info_sec_di
            where day='${insert_day}'

            union all

            select muid as device,
                   plat,
                   clientip as ipaddr,
                   CONCAT(unix_timestamp(serdatetime),'000') as timestamp,
                   networktype as network,
                   language
            from $dwd_pv_sec_di
            where day='${insert_day}'

            union all

            select muid as device,
                   plat,
                   clientip as ipaddr,
                   CONCAT(unix_timestamp(servertime),'000') as timestamp,
                   networktype as network,
                   '' as language
            from $dwd_log_run_new_di
            where day='${insert_day}'

            union all

            select muid as device,
                   plat,
                   clientip as ipaddr,
                   serdatetime as timestamp,
                   networktype as network,
                   '' as language
            from $dwd_t_location_sec_di
            where day='${insert_day}'
        ) unioned
        where device rlike '^[a-f0-9]{40}$'
    ) cleaned
    group by device, plat, ipaddr, timestamp
) grouped
left join
(
  select minip,
         country_code,
         province_code,
         city_code,
         area_code
  from $mapping_ip_attribute_code
  where day='$last_ip_mapping_partition'
) ip_mapping
on (get_ip_attribute(grouped.ipaddr) = ip_mapping.minip)
CLUSTER BY device;
"
