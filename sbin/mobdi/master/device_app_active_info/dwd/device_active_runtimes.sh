#!/bin/bash
: '
@owner: liuyanqiang
@describe: 设备的app日活跃明细表
@projectName:MobDI
@BusinessName:设备的app日活跃明细表-app_runtimes
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
source /home/dba/mobdi_center/conf/hive-env.sh

## 源表
#dwd_device_app_runtimes_sec_di=dm_mobdi_master.dwd_device_app_runtimes_sec_di

## 目标表
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

hive -v -e  "
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

INSERT OVERWRITE TABLE $dws_device_active_di PARTITION (day = '$day', plat, source)
SELECT muid AS device,
       trim(pkg) AS pkg,
       trim(apppkg) as apppkg,
       if(appkey is null
          or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] in ('null','NULL')
          or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] not rlike '^[0-9a-fA-FmM]{0,40}$',
          '',split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0]
       ) as appkey,
       if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)) AS appver,
       trim(ipaddr) AS ip,
       commonsdkver AS commonsdkver,
       array(map('k', '', 'v', '')) AS sdks,
       sum(runtimes) AS tot_times,
       sum(bg + fg + empty) AS active_cnt,
       sum(fg) AS front_active_cnt,
       sum(bg) AS back_active_cnt,
       concat_ws(',',sort_array(collect_set(from_unixtime(floor(clienttime / 1000), 'yyyy-MM-dd HH:mm:ss')))) as clienttime_list,
       concat_ws(',',sort_array(collect_set(serdatetime))) as servertime_list,
       plat,
       'runtimes' AS source
FROM $dwd_device_app_runtimes_sec_di
WHERE day = '$day'
and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
and pkg is not null
and trim(pkg) not in ('','null','NULL')
and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\.\_-]+)',0)
and length(trim(device)) = 40
and plat in (1, 2)
GROUP BY muid,
         trim(pkg),
         trim(apppkg),
         trim(ipaddr),
         if(appkey is null
          or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] in ('null','NULL')
          or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] not rlike '^[0-9a-fA-FmM]{0,40}$',
          '',split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0]
         ),
         if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)),
		     commonsdkver,
         plat
CLUSTER BY device;
"
