#!/bin/sh

set -e -x

date=$1

# input
dwd_wifi_scan_list_sec_di=dm_mobdi_master.dwd_wifi_scan_list_sec_di_mid

# output
dwd_wifilist_explore_sec_di=dm_mobdi_master.dwd_wifilist_explore_sec_di_mid

HADOOP_USER_NAME=dba hive -v -e"
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx7400m';
SET mapreduce.child.map.java.opts='-Xmx7400m';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx7400m';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.reuse.jvm.num.tasks=10;
set mapred.tasktracker.map.tasks.maximum=24;
set mapred.tasktracker.reduce.tasks.maximum=24;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
create temporary function get_min_ip as 'com.youzu.mob.java.udf.GetIpAttribute';
insert overwrite table $dwd_wifilist_explore_sec_di partition(day='$date')
select a.ieid,
       a.mcid,
       a.timestamp,
       lower(a.bssid) as bssid,
       a.ssid,
       a.level,
       a.connected,
       a.deviceid,
       b.country,
       b.province,
       b.city,
       a.clienttime,
       a.clientip,
       a.networktype,
       a.duid,
       a.plat,
       a.apppkg,
       b.bd_lon,
       b.bd_lat,
       b.country_code,
       b.province_code,
       b.city_code,
       b.area_code,
       b.accuracy,
       a.mid
from
(
  select ieid,
         mcid,
         serdatetime as timestamp,
         t['BSSID'] as bssid,
         t['SSID'] as ssid,
         t['level'] as level,
         t['___curConn'] as connected,
         deviceid,
         clienttime,
         clientip,
         get_min_ip(clientip) as minip,
         networktype,
         duid,
         plat,
         cast(rand()*50 as int) as rb,
         apppkg,
         mid
    from $dwd_wifi_scan_list_sec_di
    lateral view explode(list) exploded_table as t
    where day = '$date'
    and size(list) > 0
    and t['BSSID'] is not null
    and t['BSSID'] != '000000000000'
    and t['BSSID'] != '00:00:00:00:00:00'
) a
left join
(
  select minip,country,province,city,bd_lat,bd_lon,country_code,province_code,city_code,area_code,accuracy,n.rb
  from (
      select * from dm_sdk_mapping.mapping_ip_attribute_code
      where day=GET_LAST_PARTITION('dm_sdk_mapping','mapping_ip_attribute_code'))a
      join
      (select explode(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49)) as rb )n
) b
on a.minip=b.minip and a.rb=b.rb;
"
