#!/bin/sh

set -e -x

day=$1
p3months=`date -d "$day -90 days" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#input
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di
#tmp
ip_location_info=dw_mobdi_md.ip_location_info
#output
device_ip_proxy_3month=dw_mobdi_md.device_ip_proxy_3month

hive -e"

SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
SET mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

insert overwrite table $device_ip_proxy_3month
select b.device, b.ipaddr
from 
(
  select clientip
  from $ip_location_info
  where rate_300 < 0.4
  group by clientip
) as a 
inner join 
(
  select device, ipaddr
  from $dws_device_ip_info_di
  where day >= '$p3months' and day <= '$day' and ipaddr <> '' and ipaddr is not null
  group by device, ipaddr
) as b 
on a.clientip = b.ipaddr
"