#!/bin/sh
set -x -e
: '
@owner:liuyanqiang
@describe:对location_info、auto_location_info、log_wifi_info表进行bssid、ssid、ip信息聚合，ga下沉
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1

##input
#dwd_location_info_sec_di="dm_mobdi_master.dwd_location_info_sec_di"
#dwd_auto_location_info_sec_di="dm_mobdi_master.dwd_auto_location_info_sec_di"
#dwd_log_wifi_info_sec_di="dm_mobdi_master.dwd_log_wifi_info_sec_di"

##output
#dws_device_ip_bssid_sec_di="dm_mobdi_topic.dws_device_ip_bssid_sec_di"

:<<!
CREATE TABLE dm_mobdi_topic.dws_device_ip_bssid_sec_di(
  device string COMMENT '设备标示', 
  mcid string COMMENT '设备mcid地址', 
  ieid string COMMENT 'DUID(新版设备标示)', 
  clienttime string COMMENT '客户端时间datetime格式化yyyy-MM-dd HH:mm:ss', 
  clientip string COMMENT '客户端源IP地址', 
  bssid string COMMENT 'wifi的bssid', 
  ssid string COMMENT 'wifi的ssid', 
  wifi_flag int COMMENT '是否为wifi', 
  plat int COMMENT '系统平台')
COMMENT '对location_info、auto_location_info、log_wifi_info表进行bssid、ssid、ip信息聚合，ga下沉'
PARTITIONED BY ( 
  day string COMMENT '日期')
STORED AS ORC;
!

hive -v -e"
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $dws_device_ip_bssid_sec_di partition (day=$day)
select device,mcid,ieid,clienttime,clientip,bssid,ssid,wifi_flag,plat
from
(
  select trim(lower(muid)) as device,
         mcid,ieid,
         from_unixtime(CAST(clienttime/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss') as clienttime,
         clientip,
         case
           when trim(lower(cur_bssid)) in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
                or cur_bssid is null
                or regexp_replace(trim(lower(cur_bssid)), '-|:|\\\\.|\073', '') not rlike '^[0-9a-f]{12}$'
           then ''
           else trim(lower(cur_bssid))
         end as bssid,
         case
           when trim(cur_ssid) in ('<unknown ssid>','null','NULL','','0x')
                or cur_ssid is null
           then ''
           else trim(cur_ssid)
         end as ssid,
         case
           when lower(trim(networktype))='wifi'
                and trim(lower(cur_bssid)) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
                and cur_bssid is not null
                and regexp_replace(trim(lower(cur_bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
           then 1
           else 0
         end as wifi_flag,
         plat
  from $dwd_location_info_sec_di
  where day=$day
  and from_unixtime(CAST(clienttime/1000 as BIGINT),'yyyyMMdd')=$day
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
  and trim(muid)!='0000000000000000000000000000000000000000'

  union all

  select trim(lower(muid)) as device,
         mcid,ieid,
         from_unixtime(CAST(clienttime/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss') as clienttime,
         clientip,
         case
           when trim(lower(cur_bssid)) in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
                or cur_bssid is null
                or regexp_replace(trim(lower(cur_bssid)), '-|:|\\\\.|\073', '') not rlike '^[0-9a-f]{12}$'
           then ''
           else trim(lower(cur_bssid))
         end as bssid,
         case
           when trim(cur_ssid) in ('<unknown ssid>','null','NULL','','0x')
                or cur_ssid is null
           then ''
           else trim(cur_ssid)
         end as ssid,
         case
           when lower(trim(networktype))='wifi'
                and trim(lower(cur_bssid)) not in('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
                and cur_bssid is not null
                and regexp_replace(trim(lower(cur_bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
           then 1
           else 0
         end as wifi_flag,
         plat
  from $dwd_auto_location_info_sec_di
  where day=$day
  and from_unixtime(CAST(clienttime/1000 as BIGINT),'yyyyMMdd') = $day
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
  and trim(muid)!='0000000000000000000000000000000000000000'

  union all

  select trim(lower(muid)) as device,
         mcid,ieid,
         from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss') as clienttime,
         ipaddr as clientip,
         case
           when trim(lower(bssid)) in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
                or bssid is null
                or regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') not rlike '^[0-9a-f]{12}$'
           then ''
           else trim(lower(bssid))
         end as bssid,
         case
           when trim(ssid) in ('<unknown ssid>','null','NULL','','0x')
                or ssid is null
           then ''
           else trim(ssid)
         end as ssid,
         case
           when lower(trim(networktype))='wifi'
                and trim(lower(bssid)) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
                and bssid is not null
                and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
           then 1
           else 0
         end as wifi_flag,
         plat
  from $dwd_log_wifi_info_sec_di
  where day=$day
  and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd')=$day
  and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
  and trim(muid)!='0000000000000000000000000000000000000000'
) t1
group by device,mcid,ieid,clienttime,clientip,bssid,ssid,wifi_flag,plat;
"
