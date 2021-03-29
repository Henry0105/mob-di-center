#!/bin/sh



set -e -x

day=$1

p1months=`date -d "$day -30 days" +%Y%m`16
p2months=`date -d "$day -60 days" +%Y%m`16

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
# input
ip_risk_pre=${dm_mobdi_tmp}.ip_risk_pre
# output
device_ip_entropy_risk=${dm_mobdi_tmp}.device_ip_entropy_risk

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

with ip_shang_1month as(
select device, -sum(p*logp) as H_u
from 
(
  select device, ipaddr, cnt, cnt_all, cnt/cnt_all as p, log2(cnt/cnt_all) as logp
  from 
  (
    select device, ipaddr, cnt, sum(cnt) over(partition by device) as cnt_all
    from $ip_risk_pre
    where day= '$day'
  ) as ip_risk_pre 
) as b 
group by device
),
ip_shang_2month as(
select device, -sum(p*logp) as H_u
from 
(
  select device, ipaddr, cnt, cnt_all, cnt/cnt_all as p, log2(cnt/cnt_all) as logp
  from 
  (
    select device, ipaddr, cnt, sum(cnt) over(partition by device) as cnt_all
    from 
    (
      select device, ipaddr, sum(cnt) as cnt
      from $ip_risk_pre
      where day >= '$p1months' and day <= '$day'
      group by device, ipaddr
    ) as m 
  ) as a 
) as b 
group by device
),
ip_shang_3month as(
select device, -sum(p*logp) as H_u
from 
(
  select device, ipaddr, cnt, cnt_all, cnt/cnt_all as p, log2(cnt/cnt_all) as logp
  from 
  (
    select device, ipaddr, cnt, sum(cnt) over(partition by device) as cnt_all
    from 
    (
      select device, ipaddr,sum(cnt) as cnt
      from $ip_risk_pre
      where day >= '$p2months' and day <= '$day'
      group by device, ipaddr
    ) as m 
  ) as a 
) as b 
group by device
),
ip4_shang_risk as(
select device, avg(risk) as ip4_shang_risk
from 
(
  select device, H_u, H_u*1/max_hu as risk
  from 
  (
    select device, H_u, 16.909393219973644 as max_hu
    from ip_shang_1month
  ) as a 
  union all 
  select device, H_u, H_u*1/max_hu as risk
  from 
  (
    select device, H_u, 17.31331421181225 as max_hu
    from ip_shang_2month
  ) as b 
  union all 
  select device, H_u, H_u*1/max_hu as risk
  from 
  (
    select device, H_u, 17.538218123749015 as max_hu
    from ip_shang_3month
  ) as c 
) as d 
group by device
),
ip_first3_shang_1month as(
select device, -sum(p*logp) as H_u
from 
(
  select device, ipaddr, cnt, cnt_all, cnt/cnt_all as p, log2(cnt/cnt_all) as logp
  from 
  (
    select device, ipaddr, cnt, sum(cnt) over(partition by device) as cnt_all
    from 
    (
      select device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2]) as ipaddr,  sum(cnt) as cnt
      from $ip_risk_pre
      where day= '$day'
      group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
    ) as m 
  ) as a 
) as b 
group by device
),
ip_first3_shang_2month as(
select device, -sum(p*logp) as H_u
from 
(
  select device, ipaddr, cnt, cnt_all, cnt/cnt_all as p, log2(cnt/cnt_all) as logp
  from 
  (
    select device, ipaddr, cnt, sum(cnt) over(partition by device) as cnt_all
    from 
    (
      select device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2]) as ipaddr,  sum(cnt) as cnt
      from $ip_risk_pre
      where day >= '$p1months' and day <= '$day'
      group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
    ) as m 
  ) as a 
) as b 
group by device
),
ip_first3_shang_3month as(
select device, -sum(p*logp) as H_u
from 
(
  select device, ipaddr, cnt, cnt_all, cnt/cnt_all as p, log2(cnt/cnt_all) as logp
  from 
  (
    select device, ipaddr, cnt, sum(cnt) over(partition by device) as cnt_all
    from 
    (
      select device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2]) as ipaddr,  sum(cnt) as cnt
      from $ip_risk_pre
      where day >= '$p2months' and day <= '$day'
      group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
    ) as m 
  ) as a 
) as b 
group by device
),
ip3_shang_risk as(
select device, avg(risk) as ip3_shang_risk
from 
(
  select device, H_u, H_u*1/max_hu as risk
  from 
  (
    select device, H_u, 13.326455754497418 as max_hu
    from  ip_first3_shang_1month
  ) as a 
  union all 
  select device, H_u, H_u*1/max_hu as risk
  from 
  (
    select device, H_u, 13.46786884416953 as max_hu
    from ip_first3_shang_2month
  ) as b 
  union all 
  select device, H_u, H_u*1/max_hu as risk
  from 
  (
    select device, H_u, 13.549094703566178 as max_hu
    from ip_first3_shang_3month
  ) as c 
) as d 
group by device
)

insert overwrite table $device_ip_entropy_risk
select device, avg(risk) as ip_shang_risk
from 
(
  select device, ip4_shang_risk as risk
  from ip4_shang_risk
  union all 
  select device, ip3_shang_risk as risk
  from ip3_shang_risk
) as a 
group by device
"