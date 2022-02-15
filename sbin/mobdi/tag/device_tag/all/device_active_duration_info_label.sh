#!/bin/bash
: '
@owner:luost
@describe:设备应用活跃时长信息
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dwd_device_app_runtimes_sec_di=dm_mobdi_master.dwd_device_app_runtimes_sec_di
#dwd_xm_device_app_runtimes_sec_di=dm_mobdi_master.dwd_xm_device_app_runtimes_sec_di
#dwd_app_runtimes_stats_sec_di=dm_mobdi_master.dwd_app_runtimes_stats_sec_di
#dwd_back_info_sec_di=dm_mobdi_master.dwd_back_info_sec_di

#mapping表
#dim_app_name_info_orig=dim_mobdi_mapping.dim_app_name_info_orig

#输出表
#label_device_active_duration_info_di=dm_mobdi_report.label_device_active_duration_info_di

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with device_runtimes as (
    select device,pkg,source,runtimes
    from 
    (
        select device,pkg,source,runtimes,row_number() over(partition by device,pkg,source order by flag desc) as rk
        from 
        (
            select if(plat=1,muid,device) as device,pkg,apppkg as source,sum(runtimes) as runtimes,1 as flag
            from $dwd_device_app_runtimes_sec_di
            where day = '$day'
            and runtimes >= 0
            and runtimes <= 86400
            and if(plat=1,muid,device) = regexp_extract(if(plat=1,muid,device),'([a-f0-9]{40})', 0)
            and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\.\_-]+)',0)
            and pkg is not null
            and lower(trim(pkg)) not in ('','null')
            and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
            and apppkg is not null
            and lower(trim(apppkg)) not in ('','null')
            group by if(plat=1,muid,device),pkg,apppkg
            having sum(runtimes) <= 86400

            union all

            select if(plat=1,muid,deviceid) as device,packagename as pkg,apppkg as source,
                   floor(sum(totaltimeinforeground)/1000) as runtimes,2 as flag
            from $dwd_xm_device_app_runtimes_sec_di
            where day = '$day'
            and totaltimeinforeground >= 0
            and totaltimeinforeground <= 86400000
            and if(plat=1,muid,deviceid) = regexp_extract(if(plat=1,muid,deviceid),'([a-f0-9]{40})', 0)
            and trim(packagename)=regexp_extract(trim(packagename),'([a-zA-Z0-9\.\_-]+)',0)
            and packagename is not null
            and lower(trim(packagename)) not in ('','null')
            and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
            and apppkg is not null
            and lower(trim(apppkg)) not in ('','null')
            group by if(plat=1,muid,deviceid),packagename,apppkg
            having sum(totaltimeinforeground) <= 86400000
        )a
    )b
    where rk = 1
),

device_active_time as (
    select device,pkg,max(front_time) as front_time,max(runtimes) as runtimes,max(duration) as duration,source
    from 
    (
        select device,pkg,-1 as front_time,runtimes,-1 as duration,source
        from device_runtimes

        union all 

        select if(plat=1,muid,deviceid) as device,apppkg as pkg,floor(sum(runtimes)/1000) as front_time,-1 as runtimes,-1 as duration,apppkg as source
        from $dwd_back_info_sec_di
        where day = '$day'
        and runtimes >= 0
        and runtimes <= 86400000
        and if(plat=1,muid,deviceid) = regexp_extract(if(plat=1,muid,deviceid),'([a-f0-9]{40})', 0)
        and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
        and apppkg is not null
        and lower(trim(apppkg)) not in ('','null')
        group by if(plat=1,muid,deviceid),apppkg
        having sum(runtimes) <= 86400000

        union all

        select device,pkg,-1 as front_time,-1 as runtimes,floor(sum(duration)/1000) as duration,source
        from 
        (
            select deviceid as device,apppkg as pkg,
                   case
                     when (duration - last_duration) > 930000 then 900000
                     when (duration - last_duration) >= 0 and (duration - last_duration) <= 930000 then (duration - last_duration)
                     end as duration,
                   apppkg as source
            from
            (
                select if(plat=1,muid,deviceid) as deviceid,apppkg,duration,
                       lag(duration,1,0) over(partition by apppkg,if(plat=1,muid,deviceid),appver,launchat order by clienttime) as last_duration
                from $dwd_app_runtimes_stats_sec_di
                where day = '$day'
                and duration >= 0
                and duration <= 86400000
                and apppkg is not null
                and apppkg <> ''
                and if(plat=1,muid,deviceid) = regexp_extract(if(plat=1,muid,deviceid),'([a-f0-9]{40})', 0)
                and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
                and apppkg is not null
                and lower(trim(apppkg)) not in ('','null')
                and from_unixtime(cast(substr(launchat, 1, 10) as bigint), 'yyyyMMdd') = '$day'
            )a
        )b
        group by device,pkg,source
        having sum(duration) <= 86400000
    )t
    group by device,pkg,source
)

insert overwrite table $label_device_active_duration_info_di partition (day = '$day')
select a.device,a.pkg,if(b.name is null,'',b.name) as app_name,a.front_time,a.runtimes,a.duration,a.source
from device_active_time a 
left join $dim_app_name_info_orig b
on a.pkg = b.pkg;
"