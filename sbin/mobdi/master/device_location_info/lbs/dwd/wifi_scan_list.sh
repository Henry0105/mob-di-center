#!/bin/bash

set -x -e

if [[ -z "$1" ]]; then
  exit 1
fi

#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

###源表
dwd_wifilist_explore_sec_di=dm_mobdi_master.dwd_wifilist_explore_sec_di

###映射表
dim_mapping_bssid_location_mf=dm_mobdi_mapping.dim_mapping_bssid_location_mf
dim_bssid_level_connect_probability_all_mf=dm_mobdi_mapping.dim_bssid_level_connect_probability_all_mf

###中间库
wifi_scan_list_collected=dm_mobdi_tmp.wifi_scan_list_collected
wifi_scan_list_not_collected=dm_mobdi_tmp.wifi_scan_list_not_collected
wifi_scan_list_not_collected_probability=dm_mobdi_tmp.wifi_scan_list_not_collected_probability
wifi_scan_list_not_collected_high_probability=dm_mobdi_tmp.wifi_scan_list_not_collected_high_probability
wifi_scan_list_not_collected_low_probability=dm_mobdi_tmp.wifi_scan_list_not_collected_low_probability
wifi_scan_list_not_collected_low_probability_final=dm_mobdi_tmp.wifi_scan_list_not_collected_low_probability_final

###目标表
dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di


day=$1
plus_1day=`date +%Y%m%d -d "${day} +1 day"`
plus_2day=`date +%Y%m%d -d "${day} +2 day"`
echo "startday: "$day
echo "endday:   "$plus_2day

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
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_wifilist_explore_sec_di/day=${day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_wifilist_explore_sec_di/day=${plus_1day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_wifilist_explore_sec_di/day=${plus_2day}"
# ##########################################

#计算dim_mapping_bssid_location_mf表小于day最近的一个分区
last_bssid_mapping_mapping_partition=`hive -e "show partitions dm_mobdi_mapping.dim_mapping_bssid_location_mf" | awk -v day=${day} -F '=' '$2<day {print $0}'| sort| tail -n 1`
#计算dim_bssid_level_connect_probability_all_mf表小于day最近的一个分区
last_bssid_level_connect_probability_partition=`hive -e "show partitions dm_mobdi_mapping.dim_bssid_level_connect_probability_all_mf" | awk -v day=${day} -F '=' '$2<day {print $0}'| sort| tail -n 1`


hive -v -e "
SET mapreduce.map.memory.mb=6144;
SET mapreduce.map.java.opts='-Xmx6144m';
SET mapreduce.child.map.java.opts='-Xmx6144m';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=32000000;

--wifi_scan_list已连接的数据
insert overwrite table $wifi_scan_list_collected partition(day='$day')
select muid as deviceid,duid,bssid,ssid,clienttime,clientip,networktype,plat,day as processday,
       bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,timestamp
from $dwd_wifilist_explore_sec_di
where day between '$day' and '$plus_2day'
and from_unixtime(CAST(clienttime/1000 as BIGINT), 'yyyyMMdd') = '$day'
and connected='true'
and level>=-99
and level<0
and trim(bssid) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
and plat in (1,2);

--如果wifi_scan_list的list中找到一个已连接wifi，那么整条数据都要剔除，最后得到wifi_scan_list未连接数据
insert overwrite table $wifi_scan_list_not_collected partition(day='$day')
select t1.muid as deviceid,duid,bssid,ssid,t1.clienttime,clientip,networktype,plat,day as processday,
       bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,timestamp,level
from $dwd_wifilist_explore_sec_di t1
left join
(
  select deviceid,clienttime
  from $wifi_scan_list_collected
  where day='$day'
  group by deviceid,clienttime
) t2 on t1.muid=t2.deviceid and t1.clienttime=t2.clienttime
where day between '$day' and '$plus_2day'
and from_unixtime(CAST(t1.clienttime/1000 as BIGINT), 'yyyyMMdd') = '$day'
and level>=-99
and level<0
and trim(bssid) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
and trim(lower(t1.muid)) rlike '^[a-f0-9]{40}$' and trim(t1.muid)!='0000000000000000000000000000000000000000'
and plat in (1,2)
and t2.deviceid is null;

--未连接数据与dim_bssid_level_connect_probability_all_mf表进行join，得到各个嗅探wifi的连接概率，以及bssid经纬度、bssid国家省市取信息，并且只保留稳定型bssid
--最后对设备的每次嗅探列表的连接概率进行排序
insert overwrite table $wifi_scan_list_not_collected_probability partition(day='$day')
select deviceid,duid,t1.bssid,ssid,clienttime,clientip,networktype,plat,processday,
       bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,timestamp,
       bssid_mapping.lat,bssid_mapping.lon,
       bssid_mapping.country as bssid_country,bssid_mapping.province as bssid_province,bssid_mapping.city as bssid_city,
       bssid_mapping.district as bssid_district,bssid_mapping.street as bssid_street,bssid_mapping.acc as bssid_accuracy,bssid_type,
       nvl(t2.collect_probability,0.0) as collect_probability,
       row_number() over(partition by deviceid,clienttime,timestamp order by nvl(t2.collect_probability,0.0) desc) as rn
from $wifi_scan_list_not_collected t1
left join
(
  select bssid,level,collect_probability
  from $dim_bssid_level_connect_probability_all_mf
  where $last_bssid_level_connect_probability_partition
) t2 on t1.bssid=t2.bssid and t1.level=t2.level
inner join
(
  select bssid,lat,lon,country,province,city,district,street,acc,bssid_type
  from $dim_mapping_bssid_location_mf
  where $last_bssid_mapping_mapping_partition
  and bssid_type = 1
) bssid_mapping on bssid_mapping.bssid = t1.bssid
where day='$day';

--筛选连接可能性最高的bssid，并且概率超过0.4
insert overwrite table $wifi_scan_list_not_collected_high_probability partition(day='$day')
select deviceid,duid,bssid,ssid,clienttime,clientip,networktype,plat,processday,
       bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,timestamp,
       lat,lon,bssid_country,bssid_province,bssid_city,bssid_district,bssid_street,bssid_accuracy,bssid_type
from $wifi_scan_list_not_collected_probability
where day='$day'
and collect_probability>=0.4
and rn=1;

--剔除连接概率高的数据，得到所有连接概率低的数据
insert overwrite table $wifi_scan_list_not_collected_low_probability partition(day='$day')
select t1.deviceid,duid,t1.bssid,ssid,t1.clienttime,clientip,networktype,plat,processday,
       bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,timestamp,
       lat,lon,bssid_country,bssid_province,bssid_city,bssid_district,bssid_street,bssid_accuracy,bssid_type,
       collect_probability,rn
from $wifi_scan_list_not_collected_probability t1
left join
(
  select deviceid,clienttime
  from $wifi_scan_list_not_collected_high_probability
  where day='$day'
  group by deviceid,clienttime
) t2 on t1.deviceid=t2.deviceid and t1.clienttime=t2.clienttime
where t1.day='$day'
and t2.deviceid is null;
"

#对连接概率低的经纬度数据进行geohash7处理，只保留聚类后geohash7数量最多的经纬度信息
#同一条嗅探数据并且geohash7也相同，取这些点的经纬度的平均值
#最后保留连接概率最高的bssid信息，得到连接概率低的数据的最终结果
hive -v -e"
SET mapreduce.map.memory.mb=6144;
SET mapreduce.map.java.opts='-Xmx6144m';
SET mapreduce.child.map.java.opts='-Xmx6144m';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=32000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

insert overwrite table $wifi_scan_list_not_collected_low_probability_final partition(day='$day')
select deviceid,duid,bssid,ssid,clienttime,clientip,networktype,plat,processday,
       bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,timestamp,
       lat,lon,bssid_country,bssid_province,bssid_city,bssid_district,bssid_street,bssid_accuracy,collect_probability
from
(
  select t4.deviceid,duid,bssid,ssid,t4.clienttime,clientip,networktype,plat,processday,
         bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,apppkg,t4.timestamp,
         t3.lat,t3.lon,bssid_country,bssid_province,bssid_city,bssid_district,bssid_street,bssid_accuracy,
         collect_probability,t3.geohash7,
         row_number() over(partition by t4.deviceid,t4.clienttime,t4.timestamp order by collect_probability desc) as rn_new
  from
  (
    select deviceid,clienttime,timestamp,geohash7,lat,lon
    from
    (
      select deviceid,clienttime,timestamp,geohash7,geohash7_cnt,lat,lon,
             row_number() over(partition by deviceid,clienttime,timestamp order by geohash7_cnt desc) as geohash7_rn
      from
      (
        select deviceid,clienttime,timestamp,
               get_geohash(lat, lon, 7) as geohash7,
               count(1) as geohash7_cnt,
               avg(lat) as lat,
               avg(lon) as lon
        from $wifi_scan_list_not_collected_low_probability
        where day='$day'
        group by deviceid,clienttime,timestamp,get_geohash(lat, lon, 7)
      ) t1
    ) t2
    where geohash7_rn=1
  ) t3
  inner join
  $wifi_scan_list_not_collected_low_probability t4
  on t4.day='$day' and t3.deviceid=t4.deviceid and t3.clienttime=t4.clienttime and t3.timestamp=t4.timestamp and
     t3.geohash7=get_geohash(t4.lat, t4.lon, 7)
) t5
where rn_new=1;
"

#下面计算最终的device_location_daily
#scan_list本身已经连接的类型记为1，连接但是没有找到bssid经纬度用ip数据代替
#本身未连接但是连接可能性最高的bssid的概率超过0.4记为2
#本身未连接且连接概率低于0.4的数据，用geohash7找出聚类点最多的数据取平均经纬度数据记为3
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
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=32000000;

insert overwrite table $dwd_device_location_info_di partition (day='$day', source_table='wifi_scan_list')
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
        nvl(country,'') as country,
        nvl(province,'') as province,
        nvl(city,'') as city,
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
        abnormal_flag,
        0 as ga_abnormal_flag
    from (
        select device,
               duid,
               coalesce(bssid_location.lat, bd_lat, '') as lat,
               coalesce(bssid_location.lon, bd_lon, '') as lon,
               time, processtime,
               coalesce(bssid_location.country, ip_country_code, '') as country,
               coalesce(bssid_location.province, ip_province_code, '') as province,
               coalesce(bssid_location.city, ip_city_code, '') as city,
               coalesce(bssid_location.district, ip_area_code, '') as area,
               coalesce(bssid_location.street, '') as street,
               plat, network,
               case when bssid_location.lat is null or bssid_location.lon is null then 'ip' else type end as type,
               data_source,
               case when bssid_location.lat is null or bssid_location.lon is null then concat('ip=', ipaddr) else orig_note1 end as orig_note1,
               case when bssid_location.lat is null or bssid_location.lon is null then '' else orig_note2 end as orig_note2,
               coalesce(bssid_location.accuracy, ip_accuracy) as accuracy,
               apppkg,
               case when bssid_location.lat is null or bssid_location.lon is null then '' else orig_note3 end as orig_note3,
               2 as abnormal_flag
        from (
            select device, duid,
                   bssid_mapping.lat as lat,
                   bssid_mapping.lon as lon,
                   time, processtime, plat, network, type, data_source, orig_note1, orig_note2,
                   bssid_mapping.acc as accuracy,
                   bssid_mapping.country,
                   bssid_mapping.province,
                   bssid_mapping.city,
                   bssid_mapping.district,
                   bssid_mapping.street,
                   ipaddr,apppkg,orig_note3,
                   bd_lat,bd_lon,ip_country_code,ip_province_code,ip_city_code,ip_area_code,ip_accuracy
            from (
                select deviceid as device, duid,
                       from_unixtime(CAST(clienttime/1000 as BIGINT), 'HH:mm:ss') as time,
                       processday as processtime, plat, networktype as network,
                       'wifi_scan_list' as type,
                       'wifi_scan_list' as data_source,
                       concat('bssid=', bssid) as orig_note1,
                       concat('ssid=', ssid) as orig_note2,
                       bssid, clientip as ipaddr, apppkg, bd_lat, bd_lon,
                       country_code as ip_country_code,
                       province_code as ip_province_code,
                       city_code as ip_city_code,
                       area_code as ip_area_code,
                       accuracy as ip_accuracy,
                       'wifi_connect_type=1' as orig_note3
                from $wifi_scan_list_collected
                where day='$day'
            ) wifi_scan_list
            left join (
                select bssid,lat,lon,acc,country,province,city,district,street,bssid_type
                from $dim_mapping_bssid_location_mf
                where $last_bssid_mapping_mapping_partition
            ) bssid_mapping
            on (bssid_mapping.bssid = wifi_scan_list.bssid)  --根据bssid信息关联
        ) bssid_location
    ) a
union all
   select
     trim(lower(device)) device,
     duid,
     if(lat is null or lat > 90 or lat < -90, '', lat) as lat,
     if(lon is null or lon> 180 or lon< -180, '', lon) as lon,
     time,
     processtime,
     nvl(country,'') as country,
     nvl(province,'') as province,
     nvl(city,'') as city,
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
     abnormal_flag,
     0 as ga_abnormal_flag
   from (
       select deviceid as device, duid, lat, lon,
              from_unixtime(CAST(clienttime/1000 as BIGINT), 'HH:mm:ss') as time,
              processday as processtime,
              bssid_country as country, bssid_province as province, bssid_city as city,
              bssid_district as area, bssid_street as street,
              plat, networktype as network,
              'wifi_scan_list' as type,
              'wifi_scan_list' as data_source,
              concat('bssid=', bssid) orig_note1,
              concat('ssid=', ssid) as orig_note2,
              nvl(bssid_accuracy, '') as accuracy,
              apppkg, orig_note3,
              2 as abnormal_flag
       from (
           select deviceid,bssid,ssid,clienttime,clientip,networktype,duid,plat,processday,apppkg,
                  lat,lon,bssid_country,bssid_province,bssid_city,bssid_district,bssid_street,bssid_accuracy,
                  'wifi_connect_type=2' as orig_note3
           from $wifi_scan_list_not_collected_high_probability
           where day='$day'
       union all
           select deviceid,bssid,ssid,clienttime,clientip,networktype,duid,plat,processday,apppkg,
                  lat,lon,bssid_country,bssid_province,bssid_city,bssid_district,bssid_street,bssid_accuracy,
                  'wifi_connect_type=3' as orig_note3
           from $wifi_scan_list_not_collected_low_probability_final
           where day='$day'
        ) t1
    ) b
) device_di
group by  device,duid,lat,lon,time,processtime,country,province,city,area,street,plat,network,type,data_source,orig_note1,orig_note2,accuracy,apppkg,orig_note3,abnormal_flag,ga_abnormal_flag
;
"


