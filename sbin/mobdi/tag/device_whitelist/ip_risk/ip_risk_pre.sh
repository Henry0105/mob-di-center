#!/bin/sh

set -e -x

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

# input
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di
# output
ip_risk_pre=${dm_mobdi_tmp}.ip_risk_pre

day=$1

p1months=`date -d "$day -30 days" +%Y%m`16


hive -e"
insert overwrite table $ip_risk_pre partition(day=$day)
select device, ipaddr,count(1) as cnt
   from $dws_device_ip_info_di
   where day >'$p1months' and day <= '$day' and ipaddr <> '' and ipaddr is not null
group by device, ipaddr
"