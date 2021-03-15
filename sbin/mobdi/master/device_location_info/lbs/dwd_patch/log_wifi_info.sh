#!/bin/bash

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

###源表
dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di

###映射表
dim_latlon_blacklist_mf=dm_mobdi_mapping.dim_latlon_blacklist_mf
mapping_ip_attribute_code=dm_sdk_mapping.mapping_ip_attribute_code
dim_mapping_bssid_location_clear_mf=dm_mobdi_mapping.dim_mapping_bssid_location_clear_mf

###目标表
dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di

day=$1
echo "startday: "$day

# check source data: #######################
CHECK_DATA()
{
  local src_path=$1
  hadoop fs -test -e $src_path
  if [[ $? -eq 0 ]] ; then
    # path存在
    src_data_du=`hadoop fs -du -s $src_path | awk '{print $1}'`
    # 文件夹大小不为0
    if [[ $src_data_du != 0 ]] ;then
      return 0
    else
      return 1
    fi
  else
      return 1
  fi
}
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_log_wifi_info_sec_di/day=${day}"
# ##########################################

#bssid_mapping_sql="
#    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
#    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
#    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_mapping_bssid_location_clear_mf', 'day');
#"
#last_bssid_mapping_mapping_partition=(`hive -e "$bssid_mapping_sql"`)
#获取小于当前日期的最大分区
par_arr=(`hive -e "show partitions dm_mobdi_mapping.dim_mapping_bssid_location_clear_mf" |awk -F '=' '{print $2}'|xargs`)
for par in ${par_arr[@]}
do
  if [ $par -le $day ]
  then
    last_bssid_mapping_mapping_partition=$par
  else
    break
  fi
done

ip_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_sdk_mapping', 'mapping_ip_attribute_code', 'day');
"
last_ip_mapping_partition=(`hive -e "$ip_mapping_sql"`)

#ip_mapping_sql="
#    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
#    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
#    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_latlon_blacklist_mf', 'day');
#"
#last_ip_mapping_partition_latlon=(`hive -e "$ip_mapping_sql"`)
#获取小于当前日期的最大分区
par_arr=(`hive -e "show partitions dm_mobdi_mapping.dim_latlon_blacklist_mf" |awk -F '=' '{print $2}'|xargs`)
for par in ${par_arr[@]}
do
  if [ $par -le $day ]
  then
    last_ip_mapping_partition_latlon=$par
  else
    break
  fi
done


hive -v -e"
SET mapreduce.map.memory.mb=6144;
SET mapreduce.map.java.opts='-Xmx6144m';
SET mapreduce.child.map.java.opts='-Xmx6144m';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';

with log_wifi_info as (
  select
      nvl(muid, '') as device,
      duid,
      from_unixtime(CAST(datetime/1000 as BIGINT), 'HH:mm:ss') as time,
      day as processtime,
      plat,
      networktype as network,
      'wifi' as type,
      'wifi' as data_source,
      concat('bssid=', bssid) as orig_note1,
      concat('ssid=', ssid) as orig_note2,
      bssid,
      ipaddr,
      apppkg,CONCAT(unix_timestamp(serdatetime),'000') as serdatetime,language
  from $dwd_log_wifi_info_sec_di
  where day = '$day'
  and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd') = '$day'
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
  and plat in (1,2)
)

insert overwrite table $dwd_device_location_info_di partition (day='$day', source_table='log_wifi_info')
select
    nvl(device,'') as device,
    nvl(duid,'') as duid,
    nvl(lat,'') as lat,
    nvl(lon,'') as lon,
    nvl(time,'') as time,
    nvl(processtime,'') as processtime,
    nvl(country,'') as country,
    nvl(province,'') as province,
    nvl(city,'') as city,
    nvl(area,'') as area,
    nvl(street,'') as street,
    nvl(plat,'') as plat,
    nvl(network,'') as network,
    nvl(type,'') as type,
    nvl(data_source,'') as data_source,
    nvl(orig_note1,'') as orig_note1,
    nvl(orig_note2,'') as orig_note2,
    nvl(accuracy,'') as accuracy,
    nvl(apppkg,'') as apppkg,
    nvl(orig_note3,'') as orig_note3,
    nvl(abnormal_flag,'') as abnormal_flag,
    nvl(ga_abnormal_flag,'') as ga_abnormal_flag
from (
    select
        trim(lower(device)) device,
        duid,
        if(a.lat is null or a.lat > 90 or a.lat < -90, '', a.lat) as lat,
        if(a.lon is null or a.lon> 180 or a.lon< -180, '', a.lon) as lon,
        time,
        processtime,
        country,
        province,
        city,
        area,
        street,
        plat,
        if(network is null or (trim(lower(network)) not rlike '^(2g)|(3g)|(4g)|(5g)|(cell)|(wifi)|(bluetooth)$'),'',trim(lower(network))) as network,
        type,
        data_source,
        orig_note1,
        orig_note2,
        accuracy,
        if(apppkg is null or trim(apppkg) in ('null','NULL') or trim(apppkg)!=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0),'',trim(apppkg)) as apppkg,
        orig_note3,
        case when b.lat is not null and b.lon is not null and b.stage is not null then 1 else 0 end  as  abnormal_flag,
        ga_abnormal_flag
    from (select
               device, duid,lat,lon,time, processtime,country,province,city,area,street,
               plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg, orig_note3,ipaddr,serdatetime,language,ga_abnormal_flag,
               case when type='wifi' and split(orig_note3,'=')[1] = '1' and orig_note3 is not null then 'B'
                    when type='ip' and network='wifi' and  area!='' and network is not null and area is not null then 'C'
                    when type='wifi' and not (split(orig_note3,'=')[1] = '1' and orig_note3 is not null) then'C'
                    when type='ip' and not (network ='wifi' and area!='' and network is not null and area is not null ) then 'D' else 0 end as stage
          from (select
                    device, duid,
                    coalesce(bssid_location.lat, ip_mapping.bd_lat, '') as lat,
                    coalesce(bssid_location.lon, ip_mapping.bd_lon, '') as lon,
                    time, processtime,
                    coalesce(bssid_location.country, ip_mapping.country_code, '') as country,
                    coalesce(bssid_location.province, ip_mapping.province_code, '') as province,
                    coalesce(bssid_location.city, ip_mapping.city_code, '') as city,
                    coalesce(bssid_location.district, ip_mapping.area_code, '') as area,
                    coalesce(bssid_location.street, '') as street,
                    plat, network,
                    case when bssid_location.lat is null or bssid_location.lon is null then 'ip' else type end as type,
                    data_source,
                    case when bssid_location.lat is null or bssid_location.lon is null then concat('ip=', ipaddr) else orig_note1 end as orig_note1,
                    case when bssid_location.lat is null or bssid_location.lon is null then '' else orig_note2 end as orig_note2,
                    coalesce(bssid_location.accuracy, ip_mapping.accuracy) as accuracy,
                    apppkg,
                    case when bssid_location.lat is null or bssid_location.lon is null then '' else orig_note3 end as orig_note3,ipaddr,serdatetime,language,
                    ga_abnormal_flag
                from (select
                          device, duid,
                          bssid_mapping.lat as lat,
                          bssid_mapping.lon as lon,
                          time, processtime, plat, network, type, data_source, orig_note1, orig_note2,
                          bssid_mapping.acc as accuracy,
                          bssid_mapping.country,
                          bssid_mapping.province,
                          bssid_mapping.city,
                          bssid_mapping.district,
                          bssid_mapping.street,
                          ipaddr,apppkg,
                          concat('bssid_type=',nvl(bssid_mapping.bssid_type, 0)) as orig_note3,serdatetime,language,
                          if(bssid_mapping.flag is null,0,bssid_mapping.flag) as ga_abnormal_flag
                      from log_wifi_info
                      left join (select * from $dim_mapping_bssid_location_clear_mf where day = '$last_bssid_mapping_mapping_partition') bssid_mapping
                      on (bssid_mapping.bssid = log_wifi_info.bssid)  --根据bssid信息关联
                ) bssid_location
                left join (select * from $mapping_ip_attribute_code where day='$last_ip_mapping_partition') ip_mapping
                on (case when bssid_location.lat is not null and bssid_location.lon is not null then concat('', rand()) else get_ip_attribute(bssid_location.ipaddr) end = ip_mapping.minip)  --未关联上的再根据ip信息关联
          )tt
    ) a
    left join (select lat,lon,stage from $dim_latlon_blacklist_mf where day='$last_ip_mapping_partition_latlon') b
    on round(a.lat,5)=round(b.lat,5)
    and round(a.lon,5)=round(b.lon,5)
    and b.stage=a.stage
)a
group by  device,duid,lat,lon,time,processtime,country,province,city,area,street,plat,network,type,data_source,orig_note1,orig_note2,accuracy,apppkg,orig_note3,abnormal_flag,ga_abnormal_flag
;
"
