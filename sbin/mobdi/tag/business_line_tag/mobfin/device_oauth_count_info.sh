#!/bin/sh

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

day=$1

p1week=`date -d "$day -7 day" +%Y%m%d`
p2weeks=`date -d "$day -14 day" +%Y%m%d`
p1month=`date -d "$day -30 day" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
#dwd_log_oauth_new_di=dm_mobdi_master.dwd_log_oauth_new_di

## 目标表
#label_l1_anticheat_device_oauth_wi=dm_mobdi_report.label_l1_anticheat_device_oauth_wi

hive -v -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=256000000;

with oauthcount_1week as(
select deviceid as device, count(*) as cnt 
from 
(
  select deviceid, snsplat, clienttime, apppkg, appver
  from $dwd_log_oauth_new_di
  where day > '$p1week' and day <= '$day'
  and plat = 1 and snsplat <= 65 and snsplat is not null
  group by deviceid, snsplat, clienttime, apppkg, appver
  having count(*) = 1
) as a 
group by deviceid
),
oauthcount_2weeks as(
select deviceid as device, count(*) as cnt 
from 
(
  select deviceid, snsplat, clienttime, apppkg, appver
  from $dwd_log_oauth_new_di
  where day > '$p2weeks' and day <= '$day'
  and plat = 1 and snsplat <= 65 and snsplat is not null
  group by deviceid, snsplat, clienttime, apppkg, appver
  having count(*) = 1
) as a 
group by deviceid
),
oauthcount_1month as(
select deviceid as device, count(*) as cnt 
from 
(
  select deviceid, snsplat, clienttime, apppkg, appver
  from $dwd_log_oauth_new_di
  where day > '$p1month' and day <= '$day'
  and plat = 1 and snsplat <= 65 and snsplat is not null
  group by deviceid, snsplat, clienttime, apppkg, appver
  having count(*) = 1
) as a 
group by deviceid
)

insert overwrite table $label_l1_anticheat_device_oauth_wi partition(day='$day')
select
     coalesce( a.device, b.device) as device,
	 nvl(a.cnt,0) as cnt_7,
     nvl(b.cnt_14, 0) as cnt_14,
     nvl(b.cnt_30, 0) as cnt_30
from
 (select device, cnt from oauthcount_1week) a
 full join
 ( select
        coalesce( t1.device, t2.device) as device,
    	nvl(t1.cnt, 0) as cnt_30,
    	nvl(t2.cnt, 0) as cnt_14
    from
    (select device, cnt from oauthcount_1month) t1
    full join
    (select device, cnt from oauthcount_2weeks) t2
    on t1.device = t2.device
 )  b
on a.device = b.device
;
"
