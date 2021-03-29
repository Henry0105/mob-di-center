#!/bin/sh

day=$1
pday=`date -d "$day -30 days" +%Y%m%d`
p180day=`date -d "$day -180 days" +%Y%m%d`
day2=`date -d "$day" +%Y-%m-%d`

source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#input
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di

#md
device_active_day_info=${dm_mobdi_tmp}.device_active_day_info

#out
device_active_risk=${dm_mobdi_tmp}.device_active_risk


hive -e"

insert overwrite table $device_active_day_info
select nvl(full_info.device,one_day_info.device) as device,
       nvl(full_info.first_active_day,min_day)as first_active_day,
       case when one_day_info.device is not null then max_day
            else full_info.last_active_day
       end as last_active_day,
       case when full_info.device is null and one_day_info.device is not null then date_diff
            when full_info.device is not null and one_day_info.device is null then full_info.max_active_day_interval
            when datediff(from_unixtime(unix_timestamp(min_day,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(full_info.last_active_day,'yyyyMMdd'),'yyyy-MM-dd')) > full_info.max_active_day_interval
              then if(datediff(from_unixtime(unix_timestamp(min_day,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(full_info.last_active_day,'yyyyMMdd'),'yyyy-MM-dd'))>date_diff,
              datediff(from_unixtime(unix_timestamp(min_day,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(full_info.last_active_day,'yyyyMMdd'),'yyyy-MM-dd')),
              date_diff)
            else if(full_info.max_active_day_interval>date_diff,full_info.max_active_day_interval,date_diff)
        end as max_active_day_interval
from $device_active_day_info full_info
full join
(
  select deviceid as device,min(day) as min_day,max(day) as max_day,
         datediff(from_unixtime(unix_timestamp(max(day),'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(min(day),'yyyyMMdd'),'yyyy-MM-dd')) as date_diff
  from
  (
    select muid as deviceid,day
    from $dwd_log_run_new_di
    where day between '$pday' and '$day'
    and plat=1
    and muid is not null and length(muid)= 40 and muid = regexp_extract(muid,'([a-f0-9]{40})', 0)
     
    union all
    
    select muid as deviceid,day
    from $dwd_pv_sec_di
    where day between '$pday' and '$day'
    and plat=1
    and muid is not null and length(muid)= 40 and muid = regexp_extract(muid,'([a-f0-9]{40})', 0)
  ) t1
  group by deviceid
) one_day_info 
on full_info.device=one_day_info.device
"

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with device_active_day_info_all as(
  select device, first_active_day, last_active_day, max_active_day_interval, datediff(from_unixtime(unix_timestamp(last_active_day, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(first_active_day, 'yyyyMMdd'), 'yyyy-MM-dd')) as diff_last_first_day, datediff('$day2', from_unixtime(unix_timestamp(first_active_day, 'yyyyMMdd'), 'yyyy-MM-dd')) as diff_now_first_day, datediff('$day2', from_unixtime(unix_timestamp(last_active_day, 'yyyyMMdd'), 'yyyy-MM-dd')) as diff_now_last_day
  from $device_active_day_info
)

insert overwrite  table $device_active_risk
select device, first_active_day, last_active_day, max_active_day_interval, diff_last_first_day, diff_now_last_day, diff_now_first_day, flag, 
case 
  when flag = 1 then 0 
  when flag = 2 then 0.7
  when flag = 3 then 0
  when flag = 4 and max_active_day_interval > 180 and diff_now_last_day > 180 then exp((-10 + (max_active_day_interval - 181)*20/(368 - 181)) + 5)/(2*(exp((-10 + (max_active_day_interval - 181)*20/(368 - 181)) + 5) + 1)) + exp((-10 + (diff_now_last_day - 181)*20/(368 - 181)) + 5)/(2*(exp((-10 + (diff_now_last_day - 181)*20/(368 - 181)) + 5) + 1))
  when flag = 4 and max_active_day_interval > 180 and diff_now_last_day <= 180 then exp((-10 + (max_active_day_interval - 181)*20/(368 - 181)) + 5)/(exp((-10 + (max_active_day_interval - 181)*20/(368 - 181)) + 5) + 1)
  when flag = 4 and max_active_day_interval <= 180 and diff_now_last_day > 180 then exp((-10 + (diff_now_last_day - 181)*20/(368 - 181)) + 5)/(exp((-10 + (diff_now_last_day - 181)*20/(368 - 181)) + 5) + 1)
end as active_risk
from 
(
  select device, first_active_day, last_active_day, max_active_day_interval, diff_last_first_day, diff_now_last_day, diff_now_first_day, 
  case 
    when first_active_day >= '$p180day' then 1
    when first_active_day < '$p180day' and max_active_day_interval = diff_last_first_day and max_active_day_interval = 0 then 2
    when first_active_day < '$p180day' and max_active_day_interval <> 0 and max_active_day_interval <= 180 and diff_now_last_day <= 180 then 3
    when first_active_day < '$p180day' and max_active_day_interval <> 0 and (max_active_day_interval > 180 or diff_now_last_day > 180) then 4
  end as flag 
  from device_active_day_info_all
) t
"