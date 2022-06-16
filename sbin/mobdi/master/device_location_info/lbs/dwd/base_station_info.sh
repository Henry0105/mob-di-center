#!/bin/bash

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

###源表
dwd_base_station_info_sec_di=dm_mobdi_master.dwd_base_station_info_sec_di

###映射表
dim_latlon_blacklist_mf=dm_mobdi_mapping.dim_latlon_blacklist_mf
mapping_ip_attribute_code=dm_sdk_mapping.mapping_ip_attribute_code
dim_base_dbscan_result_month=dm_mobdi_mapping.dim_base_dbscan_result_month

###目标表
dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di


day=$1
plus_1day=`date +%Y%m%d -d "${day} +1 day"`
plus_2day=`date +%Y%m%d -d "${day} +2 day"`
echo "startday: "$day
echo "endday:   "$plus_2day


# ##########################################

base_station_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_base_dbscan_result_month', 'day');
"
last_base_station_mapping_partition=(`hive -e "$base_station_mapping_sql"`)

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
# shellcheck disable=SC2068
for par in ${par_arr[@]}
do
  if [ $par -le $day ]
  then
    last_ip_mapping_partition_latlon=$par
  else
    break
  fi
done

HADOOP_USER_NAME=dba hive -v -e"
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
set mapreduce.job.queuename=root.yarn_data_compliance2;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';

with base_station_info1 as ( --移动,联通
  select
      nvl(muid, '') as device,
      duid,
      from_unixtime(CAST(datetime/1000 as BIGINT), 'HH:mm:ss') as time,
      day as processtime,
      plat,
      networktype as network,
      'base' as type,
      'base' as data_source,
      0 as accuracy,
      lac,
      cell,
      case when carrier in ('46000', '46002', '46004', '46007', '46008') then 0         --移动
           when carrier in ('46001', '46006', '46009', '46010') then 1
           else 3 end as flag,
      ipaddr,
       apppkg,case when serdatetime='-1' or length(trim(serdatetime))<2 then '' else CONCAT(unix_timestamp(serdatetime),'000') end as serdatetime,language
  from $dwd_base_station_info_sec_di
  where day between '$day' and '$plus_2day'
  and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd') = '$day'
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
  and plat != '2'
  and ((lac is not null or  cell is not null) and (bid is  null and sid is  null and nid is  null))
  union all
  select
      nvl(device, '') as device,
      duid,
      from_unixtime(CAST(datetime/1000 as BIGINT), 'HH:mm:ss') as time,
      day as processtime,
      plat,
      networktype as network,
      'base' as type,
      'base' as data_source,
      0 as accuracy,
      lac,
      cell,
      case when carrier in ('46000', '46002', '46004', '46007', '46008') then 0         --移动
           when carrier in ('46001', '46006', '46009', '46010') then 1
           else 3 end as flag,
      ipaddr,
       apppkg,case when serdatetime='-1' or length(trim(serdatetime))<2 then '' else CONCAT(unix_timestamp(serdatetime),'000') end as serdatetime,language
  from $dwd_base_station_info_sec_di
  where day between '$day' and '$plus_2day'
  and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd') = '$day'
  and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
  and plat = '2'
  and ((lac is not null or  cell is not null) and (bid is  null and sid is  null and nid is  null))
),
base_station_info2 as ( --电信
  select
      nvl(muid, '') as device,
      duid,
      from_unixtime(CAST(datetime/1000 as BIGINT), 'HH:mm:ss') as time,
      day as processtime,
      plat,
      networktype as network,
      'base' as type,
      'base' as data_source,
      0 as accuracy,
      case when carrier in ('46003', '46005', '46011', '46012') then 2 else 3 end as flag,
      ipaddr,
       apppkg,
       nid,
       bid,
       sid,CONCAT(unix_timestamp(serdatetime),'000') as serdatetime,language
  from $dwd_base_station_info_sec_di
  where day between '$day' and '$plus_2day'
  and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd') = '$day'
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
  and plat != '2'
  and ((bid is not null or  sid is not null or  nid is not null) and (lac is  null and cell is  null ))
  union all
  select
      nvl(device, '') as device,
      duid,
      from_unixtime(CAST(datetime/1000 as BIGINT), 'HH:mm:ss') as time,
      day as processtime,
      plat,
      networktype as network,
      'base' as type,
      'base' as data_source,
      0 as accuracy,
      case when carrier in ('46003', '46005', '46011', '46012') then 2 else 3 end as flag,
      ipaddr,
       apppkg,
       nid,
       bid,
       sid,CONCAT(unix_timestamp(serdatetime),'000') as serdatetime,language
  from $dwd_base_station_info_sec_di
  where day between '$day' and '$plus_2day'
  and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd') = '$day'
  and trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
  and plat = '2'
  and ((bid is not null or  sid is not null or  nid is not null) and (lac is  null and cell is  null ))
),
base_station_mapping as(
  select
    lat, lon, acc, country, province, city, district, street,network as network_type,
    mcc, mnc, lac,
    cell,
    case when mnc = 0 then 0
	     when mnc = 1 then 1
		 else 2 end as flag,
    0 as ga_abnormal_flag
    from $dim_base_dbscan_result_month where day = '$last_base_station_mapping_partition'
)

insert overwrite table $dwd_device_location_info_di partition (day='$day', source_table='base_station_info')
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
    nvl(ga_abnormal_flag,'') as ga_abnormal_flag,
	'' as level
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
        ga_abnormal_flag
    from (select
              device, duid,lat,lon,time, processtime,country,province,city,area,street,
              plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg, orig_note3,ipaddr,serdatetime,language,ga_abnormal_flag,
              case  when type='base' and  network = '4g' and network is not null then 'B'
                    when (type='ip' and  network='wifi' and area<>'' and network is not null and area is not null) or (type='base' and not(network = '4g' and network is not null) )  then 'C'
                    when type='ip' and not(network='wifi' and area<>'' and network is not null and area is not null) then 'D' else '' end as stage
          from (select
                    device, duid,
                    coalesce(base_station_location.lat, ip_mapping.bd_lat, '') as lat,
                    coalesce(base_station_location.lon, ip_mapping.bd_lon, '') as lon,
                    time, processtime,
                    coalesce(base_station_location.country, ip_mapping.country_code, '') as country,
                    coalesce(base_station_location.province, ip_mapping.province_code, '') as province,
                    coalesce(base_station_location.city, ip_mapping.city_code, '') as city,
                    coalesce(base_station_location.district, ip_mapping.area_code, '') as area,
                    coalesce(base_station_location.street, '') as street,
                    plat, network,
                    case when base_station_location.lat is null or base_station_location.lon is null then 'ip' else type end as type,
                    data_source,
                    case when base_station_location.lat is null or base_station_location.lon is null then concat('ip=', ipaddr) else orig_note1 end as orig_note1,
                    case when base_station_location.lat is null or base_station_location.lon is null then '' else orig_note2 end as orig_note2,
                    coalesce(base_station_location.accuracy, ip_mapping.accuracy) as accuracy, apppkg, nvl(network_type,'') as orig_note3,ipaddr,serdatetime,language,
                    base_station_location.ga_abnormal_flag
                from (select
                          device, duid,
                          base_station_mapping.lat as lat,
                          base_station_mapping.lon as lon,
                          time, processtime, plat, network, type, data_source,
                          concat('lac,cell=', base_station_mapping.lac, ',', base_station_mapping.cell) as orig_note1,
                          concat('mcc,mnc=', base_station_mapping.mcc, ',', base_station_mapping.mnc) as orig_note2,
                          base_station_mapping.acc as accuracy,
                          base_station_mapping.country,
                          base_station_mapping.province,
                          base_station_mapping.city,
                          base_station_mapping.district,
                          base_station_mapping.street,
                          ipaddr,apppkg,network_type,serdatetime,language,
                          if(base_station_mapping.ga_abnormal_flag is null,0,base_station_mapping.ga_abnormal_flag) as ga_abnormal_flag
                      from base_station_info1
                      left join base_station_mapping
                      on (base_station_mapping.lac = base_station_info1.lac and base_station_mapping.cell = base_station_info1.cell
                      and base_station_mapping.flag = base_station_info1.flag)  --通过基站信息关联

                      union all

                      select
                          device, duid,
                          base_station_mapping.lat as lat,
                          base_station_mapping.lon as lon,
                          time, processtime, plat, network, type, data_source,
                          concat('nid,bid=', base_station_mapping.lac, ',', base_station_mapping.cell) as orig_note1,
                          concat('mcc,sid=', base_station_mapping.mcc, ',', base_station_mapping.mnc) as orig_note2,
                          base_station_mapping.acc as accuracy,
                          base_station_mapping.country,
                          base_station_mapping.province,
                          base_station_mapping.city,
                          base_station_mapping.district,
                          base_station_mapping.street,
                          ipaddr,apppkg,network_type,serdatetime,language,
                          if(base_station_mapping.ga_abnormal_flag is null,0,base_station_mapping.ga_abnormal_flag) as ga_abnormal_flag
                      from base_station_info2
                      left join  base_station_mapping
                      on (base_station_mapping.lac = base_station_info2.nid and base_station_mapping.cell = base_station_info2.bid
                      and base_station_mapping.mnc=base_station_info2.sid and base_station_mapping.flag = base_station_info2.flag)
                ) base_station_location
                left join (select * from $mapping_ip_attribute_code where day='$last_ip_mapping_partition') ip_mapping
                on (case when base_station_location.lat is not null and base_station_location.lon is not null then concat('', rand()) else get_ip_attribute(base_station_location.ipaddr) end = ip_mapping.minip) --通过ip信息关联
          ) aa
    ) a
    left join (select lat,lon,stage from $dim_latlon_blacklist_mf where day='$last_ip_mapping_partition_latlon') b
    on round(a.lat,5)=round(b.lat,5)
    and round(a.lon,5)=round(b.lon,5)
    and a.stage=b.stage
)a
 group by nvl(device,''),nvl(duid,''),nvl(lat,''),nvl(lon,''),nvl(time,''),nvl(processtime,''),nvl(country,''),nvl(province,''),nvl(city,''),nvl(area,''),nvl(street,''),nvl(plat,''),nvl(network,''),nvl(type,''),nvl(data_source,''),nvl(orig_note1,''),nvl(orig_note2,''),nvl(accuracy,''),nvl(apppkg,''),nvl(orig_note3,''),nvl(abnormal_flag,''),nvl(ga_abnormal_flag,'')
;
"

