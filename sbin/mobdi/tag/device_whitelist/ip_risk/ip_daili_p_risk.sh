#!/bin/sh

set -e -x

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
# input
ip_location_info=$tmpdb.ip_location_info
ip_risk_pre=$tmpdb.ip_risk_pre

# output
device_ip_proxy_p_risk=$tmpdb.device_ip_proxy_p_risk

day=$1

p1months=`date -d "$day -30 days" +%Y%m`16
p2months=`date -d "$day -60 days" +%Y%m`16


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

with ip_daili_1month as(
  select ip_risk_pre.device, ip_risk_pre.ipaddr
  from 
  (
    select clientip
    from $ip_location_info
    where rate_300 < 0.4
    group by clientip
  ) as ip_location 
  inner join 
  (
    select device, ipaddr
    from $ip_risk_pre
    where day= '$day'
  ) as ip_risk_pre 
  on ip_location.clientip = ip_risk_pre.ipaddr
),
ip_daili_2month as(
  select ip_risk_pre.device, ip_risk_pre.ipaddr
  from 
  (
    select clientip
    from $ip_location_info
    where rate_300 < 0.4
    group by clientip
  ) as ip_location 
  inner join 
  (
    select device, ipaddr
    from $ip_risk_pre
    where day >= '$p1months' and day <= '$day'
    group by device, ipaddr
  ) as ip_risk_pre 
  on ip_location.clientip = ip_risk_pre.ipaddr
),
ip_daili_3month as(
  select ip_risk_pre.device, ip_risk_pre.ipaddr
  from 
  (
    select clientip
    from $ip_location_info
    where rate_300 < 0.4
    group by clientip
  ) as ip_location 
  inner join 
  (
    select device, ipaddr
    from $ip_risk_pre
    where day >= '$p2months' and day <= '$day'
    group by device, ipaddr
  ) as ip_risk_pre 
  on ip_location.clientip = ip_risk_pre.ipaddr
),
ip_cnt_1month as(
  select device, count(*) as cnt
  from 
  (
    select device, ipaddr
    from $ip_risk_pre
    where day= '$day'
  ) as ip_risk_pre 
  group by device
),
ip_cnt_2month as(
  select device, count(*) as cnt
  from 
  (
    select device, ipaddr
    from $ip_risk_pre
    where day >= '$p1months' and day <= '$day'
    group by device, ipaddr
  ) as ip_risk_pre 
  group by device
),
ip_cnt_3month as(
  select device, count(*) as cnt
  from 
  (
    select device, ipaddr
    from $ip_risk_pre
    where day >= '$p2months' and day <= '$day'
    group by device, ipaddr
  ) as ip_risk_pre 
  group by device
),
ip_daili_p_1month as(
  select ip_daili_1month.device, ip_daili_1month.daili_ipcnt, ip_cnt_1month.cnt as all_ipcnt, ip_daili_1month.daili_ipcnt/ip_cnt_1month.cnt as p
  from 
  (
    select device, count(*) as daili_ipcnt
    from ip_daili_1month
    group by device
  ) as ip_daili_1month 
  left join 
  ip_cnt_1month  
  on ip_daili_1month.device = ip_cnt_1month.device
),
ip_daili_p_2month as(
  select ip_daili_2month.device, ip_daili_2month.daili_ipcnt, ip_cnt_2month.cnt as all_ipcnt, ip_daili_2month.daili_ipcnt/ip_cnt_2month.cnt as p
  from 
  (
    select device, count(*) as daili_ipcnt
    from ip_daili_2month
    group by device
  ) as ip_daili_2month 
  left join 
  ip_cnt_2month
  on ip_daili_2month.device = ip_cnt_2month.device
),
ip_daili_p_3month as(
  select ip_daili_3month.device, ip_daili_3month.daili_ipcnt, ip_cnt_3month.cnt as all_ipcnt, ip_daili_3month.daili_ipcnt/ip_cnt_3month.cnt as p
  from 
  (
    select device, count(*) as daili_ipcnt
    from ip_daili_3month
    group by device
  ) as ip_daili_3month 
  left join 
  ip_cnt_3month
  on ip_daili_3month.device = ip_cnt_3month.device
)
insert overwrite table $device_ip_proxy_p_risk
select device, max(risk) as daili_ip_p_risk
from 
(
  select device, p as risk 
  from ip_daili_p_1month
  union all 
  select device, p as risk 
  from ip_daili_p_2month
  union all 
  select device, p as risk 
  from ip_daili_p_3month
) as ip_daili_p_all 
group by device;
"

