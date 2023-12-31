#!/bin/bash
: '
@owner:liuyanqiang
@describe:聚合三个月的dwd_log_wifi_info_sec_di数据，为计算各种poi场景下的ssid匹配做数据准备
@projectName:mobdi
'

set -e -x

day=$1
p3monthDay=`date -d "$day -3 months" "+%Y%m%d"`
#导入配置文件
#source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#源表
dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di

#mapping表
dim_mapping_bssid_location_mf=dim_mobdi_mapping.dim_mapping_bssid_location_mf
mapping_area_par=dm_sdk_mapping.mapping_area_par

#中间库
ssid_match_data_prepare=dm_mobdi_tmp.ssid_match_data_prepare
city_name_combine_area_name=dm_mobdi_tmp.city_name_combine_area_name

#bssidMappingLastParStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`
bssidMappingLastParStr="day=`date -d "$day" +%Y%m11`"

#聚合三个月的dwd_log_wifi_info_sec_di数据，与dim_mapping_bssid_location_mf关联得到ssid和geohash7
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $ssid_match_data_prepare partition(day='$day')
select t1.bssid,t2.ssid,device,appear_day,connect_num,t2.geohash7
from
(
  select trim(bssid) as bssid,muid as device,day as appear_day,count(1) as connect_num
  from $dwd_log_wifi_info_sec_di
  where day>='$p3monthDay'
  and day<='$day'
  and trim(bssid) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
  and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
  group by trim(bssid),muid,day
) t1
inner join
(
  select trim(bssid) as bssid,ssid,substr(geohash8,1,7) as geohash7
  from $dim_mapping_bssid_location_mf
  where $bssidMappingLastParStr
  and cast(lon as double) > 73
  and cast(lon as double) < 136
  and cast(lat as double) > 3
  and cast(lat as double) < 54
  and trim(bssid) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
) t2 on t1.bssid=t2.bssid;
"

areaMappingLastParStr=`hive -e "show partitions $mapping_area_par" | sort| tail -n 1`
#areaMappingLastParStr="flag=20191010"


#将城市名与地区名连接起来，为了后续判断匹配的字符串不能是城市或者区域的名字
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $city_name_combine_area_name partition(day='$day')
select city_code,concat(city_poi,'|',area_list) as area_list
from
(
  select city_code,max(city_poi) as city_poi,concat_ws(',',collect_set(area_poi)) as area_list
  from $mapping_area_par
  where $areaMappingLastParStr
  and country='中国'
  group by city_code
)a;
"
