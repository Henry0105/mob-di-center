#!/bin/sh
set -x -e
: '
@owner:liuyanqiang
@describe:对dw_mobdi_etl.log_wifi_info表进行bssid、ssid信息聚合，ga下沉
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

day=$1

##input
#dwd_log_wifi_info_sec_di="dm_mobdi_master.dwd_log_wifi_info_sec_di"

##output
#dws_location_bssid_ssid_info_di="dm_mobdi_topic.dws_location_bssid_ssid_info_di"

hive -v -e"
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $dws_location_bssid_ssid_info_di partition(day='$day')
select trim(lower(muid)) as device,
       trim(lower(bssid)) as bssid,
       ssid,plat,
       from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyy-MM-dd HH:mm:ss') as real_date
from $dwd_log_wifi_info_sec_di
where day = '$day'
and trim(lower(bssid)) not in ('00:00:00:00:00:00','00:02:00:00:00:00','02:00:00:00:00:00','01:80:c2:00:00:03','ff:ff:ff:ff:ff:ff','11:11:11:11:11:11','00:01:02:03:04:05','00-00-00-00-00-00-00-00')
and bssid is not null
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
and trim(muid)!='0000000000000000000000000000000000000000'
group by trim(lower(muid)),
         trim(lower(bssid)),
         ssid,plat,
         from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyy-MM-dd HH:mm:ss');
"
