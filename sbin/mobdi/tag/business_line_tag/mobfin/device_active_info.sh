#!/bin/bash

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

day=$1
wday=`date -d "$day -7 days" +%Y%m%d`
day2=`date -d "$day" +%Y-%m-%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

## 源表
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di

## 目标表
#label_l1_anticheat_device_active_wf=dm_mobdi_report.label_l1_anticheat_device_active_wf

lastPartStr=`hive -e "show partitions $label_l1_anticheat_device_active_wf" | sort | tail -n 1`

if [ -z "$lastPartStr" ]; then
    lastPartStrA=$lastPartStr
fi

if [ -n "$lastPartStr" ]; then
    lastPartStrA=" AND  $lastPartStr"
fi

echo $lastPartStrA

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

insert overwrite table $label_l1_anticheat_device_active_wf partition (day='$day')
select
   device, first_active_day, last_active_day, max_active_day_interval,
   datediff('$day2', from_unixtime(unix_timestamp(last_active_day, 'yyyyMMdd'), 'yyyy-MM-dd')) as diff_now_last_day,
   datediff(from_unixtime(unix_timestamp(last_active_day, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(first_active_day, 'yyyyMMdd'), 'yyyy-MM-dd')) as diff_last_first_day
from
( select nvl(full_info.device,one_day_info.device) as device,
         nvl(full_info.first_active_day,min_day) as first_active_day,
         case when one_day_info.device is not null then max_day
                else full_info.last_active_day end as last_active_day,
         case when full_info.device is null and one_day_info.device is not null then date_diff
                 when full_info.device is not null and one_day_info.device is null then full_info.max_active_day_interval
                 when datediff(from_unixtime(unix_timestamp(min_day,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(full_info.last_active_day,'yyyyMMdd'),'yyyy-MM-dd')) > full_info.max_active_day_interval
                          then if(datediff(from_unixtime(unix_timestamp(min_day,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(full_info.last_active_day,'yyyyMMdd'),'yyyy-MM-dd'))>date_diff,
                                    datediff(from_unixtime(unix_timestamp(min_day,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(full_info.last_active_day,'yyyyMMdd'),'yyyy-MM-dd')),
                                    date_diff)
                 else if(full_info.max_active_day_interval>date_diff,full_info.max_active_day_interval,date_diff) end as max_active_day_interval                 
  from ( select device, first_active_day, last_active_day, max_active_day_interval, diff_now_last_day, diff_last_first_day
            from $device_active_wf 
            where 1=1 ${lastPartStrA} ) full_info
  full join
  (
    select deviceid as device,min(day) as min_day,max(day) as max_day,
                  coalesce(max(date_diff), 0) as date_diff
	  from			
        ( select 
        		deviceid,
        		day,
        		datediff(from_unixtime(unix_timestamp(nextday,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd')) as date_diff
        	from		
        	       ( select 
        	       		deviceid,
        	       		day,
        	       		lead(day) over(partition by deviceid order by day)  as nextday
                      from
                               (
                                 select deviceid,day
                                 from $log_run_new
                                 where day > '$wday' 
                                 and day <= '$day'
                                 and plat=1
                                 and trim(lower(deviceid)) rlike '^[a-f0-9]{40}$' and trim(deviceid)!='0000000000000000000000000000000000000000'
                                    
                                 union all
                                     
                                 select deviceid,day
                                 from $dwd_pv_sec_di
                                 where day > '$wday' 
                                 and  day <= '$day'
                                 and plat=1
                                 and trim(lower(deviceid)) rlike '^[a-f0-9]{40}$' and trim(deviceid)!='0000000000000000000000000000000000000000'
                               ) t1
        	        ) t2  
        	) t3   
       group by deviceid
  ) one_day_info 
  on full_info.device=one_day_info.device 
) join_info
;
"
