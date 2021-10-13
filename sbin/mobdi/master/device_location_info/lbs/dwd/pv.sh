#!/bin/bash

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

###源表
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
pv_db=${dwd_base_station_info_sec_di%.*}
pv_tb=${dwd_base_station_info_sec_di#*.}

###映射表
#dim_latlon_blacklist_mf=dm_mobdi_mapping.dim_latlon_blacklist_mf
#dim_mapping_ip_attribute_code=dim_sdk_mapping.dim_mapping_ip_attribute_code
#mapping_ip_attribute_code=dm_sdk_mapping.mapping_ip_attribute_code
ip_attribute_code_db=${dim_mapping_ip_attribute_code%.*}
ip_attribute_code_tb=${dim_mapping_ip_attribute_code#*.}

###目标表
#dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di

day=$1
plus_1day=`date +%Y%m%d -d "${day} +1 day"`
plus_2day=`date +%Y%m%d -d "${day} +2 day"`
echo "startday: "$day
echo "endday:   "$plus_2day

#ip_mapping_sql="
#    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
#    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
#    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_latlon_blacklist_mf', 'day');
#"
#last_ip_mapping_partition_latlon=(`hive -e "$ip_mapping_sql"`)
#获取小于当前日期的最大分区
par_arr=(`hive -e "show partitions $dim_latlon_blacklist_mf" |awk -F '=' '{print $2}'|xargs`)
for par in ${par_arr[@]}
do
  if [ $par -le $day ]
  then
    last_ip_mapping_partition_latlon=$par
  else
    break
  fi
done

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
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/$pv_db.db/$pv_tb/day=${day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/$pv_db.db/$pv_tb/day=${plus_1day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/$pv_db.db/$pv_tb/day=${plus_2day}"
# ##########################################

ip_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('$ip_attribute_code_db', '$ip_attribute_code_tb', 'day');
"
last_ip_mapping_partition=(`hive -e "$ip_mapping_sql"`)

hive -v -e "
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx4096m';
SET mapreduce.child.map.java.opts='-Xmx4096m';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=32000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';

with t_pv as (
  select
      nvl(muid, '') as device,
      duid,
      case when clienttime is not null then substring(clienttime, 12) else '' end as time,
      day as processtime,
      plat,
      networktype as network,
      'ip' as type,
      'pv' as data_source,
      concat('ip=', clientip) as orig_note1,
      '' as orig_note2,
      0 as accuracy,
      clientip as ipaddr,apppkg,case when length(unix_timestamp(serdatetime))<>10 then '' else CONCAT(unix_timestamp(serdatetime),'000') end as serdatetime,language
  from $dwd_pv_sec_di
  where day between '$day' and '$plus_2day'
  and from_unixtime(unix_timestamp(clienttime), 'yyyyMMdd') = '$day'  --yyyy-MM-dd hh:mm:ss 时间转换为时间戳再根据格式转换为日期yyyyMMdd
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
  and plat = '1'
  union all
  select
      nvl(deviceid, '') as device,
      duid,
      case when clienttime is not null then substring(clienttime, 12) else '' end as time,
      day as processtime,
      plat,
      networktype as network,
      'ip' as type,
      'pv' as data_source,
      concat('ip=', clientip) as orig_note1,
      '' as orig_note2,
      0 as accuracy,
      clientip as ipaddr,apppkg,case when length(unix_timestamp(serdatetime))<>10 then '' else CONCAT(unix_timestamp(serdatetime),'000') end as serdatetime,language
  from $dwd_pv_sec_di
  where day between '$day' and '$plus_2day'
  and from_unixtime(unix_timestamp(clienttime), 'yyyyMMdd') = '$day'  --yyyy-MM-dd hh:mm:ss 时间转换为时间戳再根据格式转换为日期yyyyMMdd
  and trim(lower(deviceid)) rlike '^[a-f0-9]{40}$' and trim(deviceid)!='0000000000000000000000000000000000000000'
  and plat = '2'
)

insert overwrite table $dwd_device_location_info_di partition (day='$day', source_table='pv')
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
       case when b.lat is not null and b.lon is not null and b.stage is not null then 1 else 0 end as abnormal_flag,
       0 as ga_abnormal_flag
    from (select  device, duid,lat,lon,time, processtime,country,province,city,area,street,
                  plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg,orig_note3,ipaddr,serdatetime,language,
                  case  when type='ip' and network='wifi' and area<>'' and  network is not null and area is not null then 'C'
                        when type='ip' and not (network ='wifi' and area<>'' and network is not null and area is not null) then 'D'
                        else '' end as stage
          from (select  device, duid,
                        coalesce(ip_mapping.bd_lat, '') as lat,
                        coalesce(ip_mapping.bd_lon, '') as lon,
                        time, processtime,
                        coalesce(ip_mapping.country_code, '') as country,
                        coalesce(ip_mapping.province_code, '') as province,
                        coalesce(ip_mapping.city_code, '') as city,
                        coalesce(ip_mapping.area_code, '') as area,
                        '' as street,
                        plat, network, type, data_source, orig_note1, orig_note2,
                        t_pv.accuracy,apppkg, '' as orig_note3,ipaddr,serdatetime,language
                from t_pv
                left join (select * from $dim_mapping_ip_attribute_code where day='$last_ip_mapping_partition') ip_mapping
                on (get_ip_attribute(t_pv.ipaddr) = ip_mapping.minip) --根据ip信息关联
                ) tt
           ) a
           left join (select lat,lon,stage from $dim_latlon_blacklist_mf
                      where day='$last_ip_mapping_partition_latlon' and stage in ('C','D')) b
           on round(a.lat,5)=round(b.lat,5)
           and round(a.lon,5)=round(b.lon,5)
           and a.stage=b.stage
)a
group by  device,duid,lat,lon,time,processtime,country,province,city,area,street,plat,network,type,data_source,orig_note1,orig_note2,accuracy,apppkg,orig_note3,abnormal_flag,ga_abnormal_flag
;
"
