#!/bin/sh

day=$1

p1months=`date -d "$day -30 day" +%Y%m%d`
p2months=`date -d "$day -60 day" +%Y%m%d`
p3months=`date -d "$day -90 day" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dwd_log_oauth_new_di=dm_mobdi_master.dwd_log_oauth_new_di
#out
device_oauthcount_risk=${dm_mobdi_tmp}.device_oauthcount_risk

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with oauthcount_1month as(
select deviceid as device, count(*) as cnt 
from 
(
  select muid as deviceid, snsplat, clienttime, apppkg, appver
  from $dwd_log_oauth_new_di
  where day >= '$p1months' and day <= '$day'
  and plat = 1 and snsplat <= 59 and snsplat is not null
  group by muid, snsplat, clienttime, apppkg, appver
  having count(*) = 1
) as a 
group by deviceid
),
oauthcount_2month as(
select deviceid as device, count(*) as cnt 
from 
(
  select muid as deviceid, snsplat, clienttime, apppkg, appver
  from $dwd_log_oauth_new_di
  where day >= '$p2months' and day <= '$day'
  and plat = 1 and snsplat <= 59 and snsplat is not null
  group by muid, snsplat, clienttime, apppkg, appver
  having count(*) = 1
) as a 
group by deviceid
),
oauthcount_3month as(
select deviceid as device, count(*) as cnt 
from 
(
  select muid as deviceid, snsplat, clienttime, apppkg, appver
  from $dwd_log_oauth_new_di
  where day >= '$p3months' and day <= '$day'
  and plat = 1 and snsplat <= 59 and snsplat is not null
  group by muid, snsplat, clienttime, apppkg, appver
  having count(*) = 1
) as a 
group by deviceid
)
insert overwrite  table $device_oauthcount_risk
select device, avg(risk) as oauth_risk
from 
(
  select device, risk
  from 
  (
    select device, 
    case 
      when cnt_3month <= (2+1.5*(2-1)) then 0
      when cnt_3month > (2+1.5*(2-1)) and cnt_3month <= 10 then cnt_3month*1/10 
      when cnt_3month > 10 then 1
    end as risk
    from 
    (
      select device, cnt as cnt_3month
      from oauthcount_3month
    ) as m1 
    union all
    select device, 
    case 
      when cnt_2month <= (2+1.5*(2-1)) then 0
      when cnt_2month > (2+1.5*(2-1)) and cnt_2month <= 10 then cnt_2month*1/10 
      when cnt_2month > 10 then 1 
    end as risk
    from 
    (
      select device, cnt as cnt_2month
      from oauthcount_2month
    ) as m2 
    union all
    select device, 
    case 
      when cnt_1month <= (2+1.5*(2-1)) then 0
      when cnt_1month > (2+1.5*(2-1)) and cnt_1month <= 10 then cnt_1month*1/10 
      when cnt_1month > 10 then 1 
    end as risk
    from 
    (
      select device, cnt as cnt_1month
      from oauthcount_1month
    ) as m3
  ) as a 
) as b 
group by device
"