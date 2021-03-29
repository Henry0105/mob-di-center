#!/bin/sh

set -e -x
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

# input
ip_risk_pre=${dm_mobdi_tmp}.ip_risk_pre
# output
device_ip_risk=${dm_mobdi_tmp}.device_ip_risk

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

with  ip_cnt_1month as(
 select device, count(*) as cnt
 from 
 (
   select device, ipaddr
   from $ip_risk_pre
   where day = $day
 ) as a 
 group by device
),
ip_cnt_2month as(
 select device, count(*) as cnt
 from 
 (
   select device, ipaddr
   from $ip_risk_pre
   where day >='$p1months' and day <= '$day'
   group by device, ipaddr
 ) as a 
 group by device
),
ip_cnt_3month as(
 select device, count(*) as cnt
 from 
 (
   select device, ipaddr
   from $ip_risk_pre
   where day >='$p2months' and day <= '$day'
   group by device, ipaddr
 ) as a 
 group by device
),
ip4_cnt_risk as(
  select device, avg(risk) as ip4_cnt_risk
  from 
  (
    select device, cnt, 
    case 
      when cnt <= (7+1.5*(7-1)) then 0
      when cnt > (7+1.5*(7-1)) and cnt <= 40 then cnt*1/(40 - (7+1.5*(7-1))) + (1 - 40*1/(40 - (7+1.5*(7-1))))
      when cnt > 40 then 1
    end as risk
    from ip_cnt_1month
    union all 
    select device, cnt, 
    case 
      when cnt <= (9+1.5*(9-1)) then 0
      when cnt > (9+1.5*(9-1)) and cnt <= 50 then cnt*1/(50 - (9+1.5*(9-1))) + (1 - 50*1/(50 - (9+1.5*(9-1))))
      when cnt > 50 then 1
    end as risk
    from ip_cnt_2month 
    union all 
    select device, cnt, 
    case 
      when cnt <= (10+1.5*(10-1)) then 0
      when cnt > (10+1.5*(10-1)) and cnt <= 60 then cnt*1/(60 - (10+1.5*(10-1))) + (1 - 60*1/(60 - (10+1.5*(10-1))))
      when cnt > 60 then 1
    end as risk
    from ip_cnt_3month 
  ) as d 
  group by device
),
ip_first3_cnt_1month as(
  select device, count(*) as cnt
  from 
  (
    select device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2]) as ipaddr
    from $ip_risk_pre
    where day= '$day'
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
  ) as a 
  group by device
),
ip_first3_cnt_2month as(
  select device, count(*) as cnt
  from 
  (
    select device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2]) as ipaddr
    from $ip_risk_pre
    where day >= '$p1months' and day <= '$day'
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
  ) as a 
  group by device
),
ip_first3_cnt_3month as(
  select device, count(*) as cnt
  from 
  (
    select device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2]) as ipaddr
    from $ip_risk_pre
    where day >= '$p2months' and day <= '$day'
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
  ) as a 
  group by device
),
ip3_cnt_risk as(
  select device, avg(risk) as ip3_cnt_risk
  from 
  (
    select device, cnt, 
    case 
      when cnt <= (5+1.5*(5-1)) then 0
      when cnt > (5+1.5*(5-1)) and cnt <= 20 then cnt*1/(20 - (5+1.5*(5-1))) + (1 - 20*1/(20 - (5+1.5*(5-1))))
      when cnt > 20 then 1
    end as risk
    from ip_first3_cnt_1month
    union all 
    select device, cnt, 
    case 
      when cnt <= (6+1.5*(6-1)) then 0
      when cnt > (6+1.5*(6-1)) and cnt <= 25 then cnt*1/(25 - (6+1.5*(6-1))) + (1 - 25*1/(25 - (6+1.5*(6-1))))
      when cnt > 25 then 1
    end as risk
    from ip_first3_cnt_2month
    union all 
    select device, cnt, 
    case 
      when cnt <= (6+1.5*(6-1)) then 0
      when cnt > (6+1.5*(6-1)) and cnt <= 25 then cnt*1/(25 - (6+1.5*(6-1))) + (1 - 25*1/(25 - (6+1.5*(6-1))))
      when cnt > 25 then 1
    end as risk
    from ip_first3_cnt_3month 
  ) as d 
  group by device
)
insert overwrite table $device_ip_risk
select device, avg(risk) as ip_risk
from 
(
  select device, ip4_cnt_risk as risk
  from ip4_cnt_risk
  union all 
  select device, ip3_cnt_risk as risk
  from ip3_cnt_risk
) as a 
group by device
"