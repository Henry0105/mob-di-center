#!/bin/bash
: '
@owner: liuyanqiang
@describe: 设备的app日活跃明细表
@projectName:MobDI
@BusinessName:设备的app日活跃明细表-app_dir_active
'

set -e -x

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

## 源表
#dwd_app_dir_active_sec_di=dm_mobdi_master.dwd_app_dir_active_sec_di

## 目标表
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

hive -v -e "
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $dwd_device_active_di PARTITION (day = '$day', plat, source)
SELECT device,
       pkg,
       apppkg,
       appkey,
       appver,
       clientip AS ip,commonsdkver,
       array(map('k', '', 'v', '')) AS sdks,
       0 AS tot_times,
       0 AS active_cnt,
       0 AS front_active_cnt,
       0 AS back_active_cnt,
       concat_ws(',', sort_array(collect_list(from_unixtime(floor(clienttime / 1000), 'yyyy-MM-dd HH:mm:ss')))) as clienttime_list,
       concat_ws(',', sort_array(collect_list(from_unixtime(floor(serdatetime / 1000), 'yyyy-MM-dd HH:mm:ss')))) as servertime_list,
       plat,
       'appdir' AS source
FROM
(
    SELECT muid as device,
           trim(expolde_pkg) AS pkg,
           update_time,
           trim(apppkg) as apppkg,
           if(appkey is null
              or trim(regexp_replace(appkey,'\"','')) in ('null','NULL')
              or trim(regexp_replace(appkey,'\"','')) not rlike '^[0-9a-fA-FmM]{0,40}$',
              '',trim(regexp_replace(appkey,'\"',''))
           ) as appkey,
           if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)) as appver,
           trim(clientip) as clientip,
           if(commonsdkver is null,'',cast(commonsdkver as string)) as commonsdkver,
           clienttime,
           serdatetime,
           plat
    FROM $dwd_app_dir_active_sec_di
    lateral view explode(update) update_map as expolde_pkg, update_time
    WHERE day = '$day'
    and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
    and trim(deviceid) != '0000000000000000000000000000000000000000'
    and plat in (1, 2)
) as t1
WHERE pkg is not null
and pkg not in ('','null','NULL')
and pkg=regexp_extract(pkg,'([a-zA-Z0-9\.\_-]+)',0)
and update_time >= unix_timestamp('$day 00:00:00', 'yyyyMMdd HH:mm:ss') * 1000
and update_time <= unix_timestamp('$day 24:00:00', 'yyyyMMdd HH:mm:ss') * 1000
GROUP BY muid,pkg,apppkg,appkey,appver,clientip,commonsdkver,plat;
"
