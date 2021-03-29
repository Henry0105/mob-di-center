#!/bin/sh

day=$1

p1months=`date -d "$day -30 day" +%Y%m%d`
p2months=`date -d "$day -60 day" +%Y%m%d`
p3months=`date -d "$day -90 day" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#input
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di

#out
device_bssidcnt_risk=${dm_mobdi_tmp}.device_bssidcnt_risk

hive -e"
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
with device_bssid_cnt_1month as(
  select device, count(*) as cnt 
  from 
  (
    select device, bssid
    from 
    (
      select device, regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') as bssid, from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
      from $dwd_log_wifi_info_sec_di
      where day >= '$p1months' and day <= '$day'
      and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$' and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') and bssid is not null 
    ) as log_wifi_info 
    where real_date >= '$p1months' and real_date <= '$day'
    group by device, bssid
  ) as b 
  group by device
),
device_bssid_cnt_2month as(
  select device, count(*) as cnt 
  from 
  (
    select device, bssid
    from 
    (
      select device, regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') as bssid, from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
      from $dwd_log_wifi_info_sec_di
      where day >= '$p2months' and day <= '$day'
      and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$' and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') and bssid is not null 
    ) as log_wifi_info 
    where real_date >= '$p2months' and real_date <= '$day'
    group by device, bssid
  ) as b 
  group by device
), 
device_bssid_cnt_3month as(
  select device, count(*) as cnt 
  from 
  (
    select device, bssid
    from 
    (
      select device, regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') as bssid, from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
      from $dwd_log_wifi_info_sec_di
      where day >= '$p3months' and day <= '$day'
      and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') rlike '^[0-9a-f]{12}$' and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') and bssid is not null 
    ) as log_wifi_info 
    where real_date >= '$p3months' and real_date <= '$day'
    group by device, bssid
  ) as b 
  group by device
)
insert overwrite table $device_bssidcnt_risk
select device, avg(risk) as bssidcnt_risk 
from 
(
  select device, cnt, 
  case 
    when cnt <= (2+1.5*(2-1)) then 0
    when cnt > (2+1.5*(2-1)) and cnt <= 10 then cnt*1/(10-(2+1.5*(2-1))) + (1 -10*1/(10-(2+1.5*(2-1))))
    when cnt > 10 then 1
  end as risk
  from 
  (
    select device, cnt
    from device_bssid_cnt_1month
  ) as m1 
  union all 
  select device, cnt, 
  case 
    when cnt <= (3+1.5*(3-1)) then 0
    when cnt > (3+1.5*(3-1)) and cnt <= 10 then cnt*1/(10-(3+1.5*(3-1))) + (1 - 10*1/(10-(3+1.5*(3-1))))
    when cnt > 10 then 1
  end as risk
  from 
  (
    select device, cnt
    from device_bssid_cnt_2month
  ) as m2 
  union all 
  select device, cnt, 
  case 
    when cnt <= (3+1.5*(3-1)) then 0
    when cnt > (3+1.5*(3-1)) and cnt <= 10 then cnt*1/(10-(3+1.5*(3-1))) + (1 - 10*1/(10-(3+1.5*(3-1))))
    when cnt > 10 then 1
  end as risk
  from 
  (
    select device, cnt
    from device_bssid_cnt_3month
  ) as m3 
) as d 
group by device
"