#!/bin/bash

set -e -x

statis_date=$1

source /home/dba/mobdi_center/conf/hive-env.sh

: '
input:$dwd_device_location_di_v2
out:mob_dashboard.ads_device_location_aggr_statiscs3_w
'

#3执行hql代码
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.parallel=true ;
set hive.exec.parallel.thread.number=6;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;
set hive.merge.mapfiles = true ;
set hive.merge.mapredfiles = true ;
set hive.merge.size.per.task = 268435456;
set hive.merge.smallfiles.avgsize=32000000 ;
set hive.auto.convert.join=true;
set hive.exec.reducers.bytes.per.reducer=300000000;
set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode = nonstrict;


INSERT OVERWRITE TABLE $ads_device_location_aggr_statiscs3_w
PARTITION(day )
	---1.全天+全国
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,day                                                   as whole_day
    ,'不限'                                                as daytime
    ,'不限'                                                as night
    ,country                                               as country
    ,'不限'                                                as province
    ,'不限'                                                as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
        day
        ,device
        ,country
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where  day = ${statis_date}   and   type in ('gps','wifi','base')
    group by day
            ,device
            ,country
) t
group by day
        ,country
        ,hour_ratio

union all

---2.全天+各省
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,day                                                   as whole_day
    ,'不限'                                                as daytime
    ,'不限'                                                as night
    ,'不限'                                                as country
    ,province                                              as province
    ,'不限'                                                as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         day
        ,device
        ,province
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day = ${statis_date}   and   type in ('gps','wifi','base')
    group by day
            ,device
            ,province
) t
group by day
        ,province
        ,hour_ratio

union all

---3.全天+各城市
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,day                                                   as whole_day
    ,'不限'                                                as daytime
    ,'不限'                                                as night
    ,'不限'                                                as country
    ,'不限'                                               as province
    ,city                                                  as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         day
        ,device
        ,city
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where     day = ${statis_date}   and   type in ('gps','wifi','base')
    group by day
            ,device
            ,city
) t
group by day
        ,city
        ,hour_ratio

union all

---4.白天+全国，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,'不限'                                                as whole_day
    ,'白天'                                                as daytime
    ,'不限'                                                as night
    ,country                                                as country
    ,'不限'                                                as province
    ,'不限'                                                as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,country
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where ay =${statis_date}
      and substr(time,1,2) >= '08' and  substr(time,1,2)<= '21'
      and type in ('gps','wifi','base')
    group by device
            ,country
) t
group by country
        ,hour_ratio

union all

---5.白天+各省份，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,'不限'                                                as whole_day
    ,'白天'                                                as daytime
    ,'不限'                                                as night
    ,'不限'                                                 as country
    ,province                                               as province
    ,'不限'                                                  as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
select
     device
    ,province
    ,count(distinct substr(time,0,2)) as hour_ratio
from $dwd_device_location_di_v2
where   day =${statis_date}
    and substr(time,1,2) >= '08' and  substr(time,1,2)<= '21'
    and type in ('gps','wifi','base')
group by device
        ,province
) t
group by province
        ,hour_ratio


union all
---6.白天+各省份，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,'不限'                                                as whole_day
    ,'白天'                                               as daytime
    ,'不限'                                                as night
    ,'不限'                                                  as country
    ,'不限'                                                  as province
    ,city                                                  as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,city
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day =${statis_date}
         and substr(time,1,2) >= '08' and  substr(time,1,2)<= '21'
         and type in ('gps','wifi','base')
    group by device
            ,city
) t
group by city
        ,hour_ratio

union all
---7.夜间+全国，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,'不限'                                                as whole_day
    ,'不限'                                                as daytime
    ,'夜间'                                               as night
    ,country                                                as country
    ,'不限'                                               as province
    ,'不限'                                                  as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,country
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day =${statis_date}
        and ((substr(time,1,2) >= '00' and  substr(time,1,2)<= '07') or substr(time,1,2) in ('22','23'))
        and type in ('gps','wifi','base')
    group by device
            ,country
) t
group by country
        ,hour_ratio

union all
---8.夜间+各省份，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,'不限'                                                as whole_day
    ,'不限'                                                as daytime
    ,'夜间'                                                as night
    ,'不限'                                                as country
    ,province                                              as province
    ,'不限'                                                as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                        as day
from (
    select
         device
        ,province
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day =${statis_date}
        and ((substr(time,1,2) >= '00' and  substr(time,1,2)<= '07') or substr(time,1,2) in ('22','23'))
        and type in ('gps','wifi','base')
    group by device
            ,province
) t
group by province
        ,hour_ratio


union all
---9.夜间+城市，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,'不限'                                                as type
    ,'不限'                                                as whole_day
    ,'不限'                                                as daytime
    ,'夜间'                                                as night
    ,'不限'                                                as country
    ,'不限'                                                 as province
    ,city                                                 as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,city
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day =${statis_date}
        and ((substr(time,1,2) >= '00' and  substr(time,1,2)<= '07') or substr(time,1,2) in ('22','23'))
        and type in ('gps','wifi','base')
    group by device
            ,city
) t
group by city
        ,hour_ratio

union all
---10 type+全天+全国，
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                  as type
    ,day                                                    as whole_day
    ,'不限'                                                as daytime
    ,'不限'                                                 as night
    ,country                                                as country
    ,'不限'                                                 as province
    ,'不限'                                                  as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         day
        ,device
        ,country
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and type in ('gps','wifi','base')
    group by day
            ,device
            ,country
            ,type
) t
group by type
        ,day
        ,country
        ,hour_ratio

union all
---11 type+全天+全省，
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                  as type
    ,day                                                    as whole_day
    ,'不限'                                                as daytime
    ,'不限'                                                 as night
    ,'不限'                                               as country
    ,province                                                as province
    ,'不限'                                                as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         day
        ,device
        ,province
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day =${statis_date}
         and type in ('gps','wifi','base')
    group by day
            ,device
            ,province
            ,type
) t
group by type
        ,day
        ,province
        ,hour_ratio

union all
---12 type+全天+全市，
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                  as type
    ,day                                                    as whole_day
    ,'不限'                                                as daytime
    ,'不限'                                                as night
    ,'不限'                                                as country
    ,'不限'                                               as province
    ,city                                                 as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         day
        ,device
        ,city
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and type in ('gps','wifi','base')
    group by day
            ,device
            ,city
            ,type
) t
group by type
        ,day
        ,city
        ,hour_ratio

union all
---13.type+白天+全国，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                  as type
    ,'不限'                                                as whole_day
    ,'白天'                                                as daytime
    ,'不限'                                                  as night
    ,country                                                as country
    ,'不限'                                              as province
    ,'不限'                                              as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,country
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and substr(time,1,2) >= '08' and  substr(time,1,2)<= '21'
        and type in ('gps','wifi','base')
    group by device
            ,country
            ,type
) t
group by country
        ,hour_ratio
        ,type

union all
---14.type+白天+各省，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                  as type
    ,'不限'                                                as whole_day
    ,'白天'                                                as daytime
    ,'不限'                                                  as night
    ,'不限'                                                 as country
    ,province                                               as province
    ,'不限'                                              as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,province
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and substr(time,1,2) >= '08' and  substr(time,1,2)<= '21'
        and type in ('gps','wifi','base')
    group by device
            ,province
            ,type
) t
group by province
        ,hour_ratio
        ,type

union all
---15.type+白天+各城市，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                  as type
    ,'不限'                                                as whole_day
    ,'白天'                                                as daytime
    ,'不限'                                               as night
    ,'不限'                                               as country
    ,'不限'                                               as province
    ,city                                              as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,city
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and substr(time,1,2) >= '08' and  substr(time,1,2)<= '21'
        and type in ('gps','wifi','base')
    group by device
            ,city
            ,type
) t
group by city
        ,hour_ratio
        ,type

union all
---16.type+夜间+全国，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                   as type
    ,'不限'                                                as whole_day
    ,'不限'                                                as daytime
    ,'夜间'                                               as night
    ,country                                                as country
    ,'不限'                                                as province
    ,'不限'                                             as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,country
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and ((substr(time,1,2) >= '00' and  substr(time,1,2)<= '07') or substr(time,1,2) in ('22','23'))
        and type in ('gps','wifi','base')
    group by device
            ,country
            ,type
) t
group by country
        ,hour_ratio
        ,type

union all
---17.type+夜间+各省，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss') as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')          as data_dt
    ,type                                                   as type
    ,'不限'                                                as whole_day
    ,'不限'                                                as daytime
    ,'夜间'                                               as night
    ,'不限'                                                as country
    ,province                                               as province
    ,'不限'                                             as city
    ,hour_ratio                                            as hour_ratio
    ,count(distinct device)                                as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,province
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where    day =${statis_date}
        and ((substr(time,1,2) >= '00' and  substr(time,1,2)<= '07') or substr(time,1,2) in ('22','23'))
        and type in ('gps','wifi','base')
    group by device
            ,province
            ,type
) t
group by province
        ,hour_ratio
        ,type

union all
---18.type+夜间+各市，白天是小时当维度
select
     from_unixtime(unix_timestamp(),'yyyy-mm-dd hh:mm:ss')  as crtd_tmst
    ,from_unixtime(unix_timestamp(),'yyyy-mm-dd')           as data_dt
    ,type                                                   as type
    ,'不限'                                                 as whole_day
    ,'不限'                                                 as daytime
    ,'夜间'                                                 as night
    ,'不限'                                                 as country
    ,'不限'                                                 as province
    ,city                                                   as city
    ,hour_ratio                                             as hour_ratio
    ,count(distinct device)                                 as anals_value
    ,${statis_date}                                         as day
from (
    select
         device
        ,city
        ,type
        ,count(distinct substr(time,0,2)) as hour_ratio
    from $dwd_device_location_di_v2
    where   day =${statis_date}
        and ((substr(time,1,2) >= '00' and  substr(time,1,2)<= '07') or substr(time,1,2) in ('22','23'))
        and type in ('gps','wifi','base')
    group by device
            ,city
            ,type
) t
group by city
        ,hour_ratio
        ,type
;
"

