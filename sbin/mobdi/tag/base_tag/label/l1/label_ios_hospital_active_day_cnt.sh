#!/bin/bash

day=$1

# 无model使用

#input
device_lbs_poi_daily=dm_mobdi_master.device_lbs_poi_daily
#output
label_ios_hospital_active_day_cnt=${label_ios_hospital_active_day_cnt}

p3monthDay=`date -d "$day -3 months" "+%Y%m%d"`

HADOOP_USER_NAME=dba hive -v -e"
insert overwrite table ${label_ios_hospital_active_day_cnt} partition(day="$day")
select idfa, count(1) as cnt
from (
    select idfa, day
    from (
        select
            device, day
        from ${device_lbs_poi_daily}
        where day > '$p3monthDay' and day <= '$day'
        and type_9 is not null
        and type_9.name is not null
        and type_9.name != ''
        and type_9.name not like '%??%'
        group by
            device, day
    ) a
    join
        dw_mobdi_md.ios_idfa_device_mapping b
    on a.device = b.device
    group by
        idfa, day
)c 
group by idfa
"
