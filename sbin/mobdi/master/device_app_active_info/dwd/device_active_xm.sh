#!/bin/bash
: '
@owner: liuyanqiang
@describe: 设备的app日活跃明细表
@projectName:MobDI
@BusinessName:设备的app日活跃明细表-xm_device_app_runtimes
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
#dwd_xm_device_app_runtimes_sec_di=dm_mobdi_master.dwd_xm_device_app_runtimes_sec_di

## 目标表
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

hive -v -e "
SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=1000;
SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=10240;
SET mapreduce.reduce.java.opts='-Xmx9g';
SET mapreduce.child.reduce.java.opts='-Xmx3g';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $dws_device_active_di PARTITION (day = '$day', plat, source)
select muid as device,
       trim(packagename) as pkg,
       trim(apppkg) AS apppkg,
       if(appkey is null
          or trim(regexp_replace(appkey,'\"','')) in ('null','NULL')
          or trim(regexp_replace(appkey,'\"','')) not rlike '^[0-9a-fA-FmM]{0,40}$',
          '',trim(regexp_replace(appkey,'\"',''))
       ) AS appkey,
       if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)) AS appver,
       trim(clientip) AS ip,
       '' as commonsdkver,
       array(map('k', '', 'v', '')) as sdks,
       0 as tot_times,0 as active_cnt,0 as front_active_cnt,0 as back_active_cnt,
       concat_ws(',',sort_array(collect_set(from_unixtime(floor(clienttime/1000),'yyyy-MM-dd HH:mm:ss')))) as clienttime_list,
       concat_ws(',',sort_array(collect_set(from_unixtime(floor(serdatetime/1000),'yyyy-MM-dd HH:mm:ss')))) as servertime_list,
       plat,
       'xm' as source
FROM $dwd_xm_device_app_runtimes_sec_di
WHERE day = '$day'
and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
and packagename is not null
and trim(packagename) not in ('','null','NULL')
and trim(packagename)=regexp_extract(trim(packagename),'([a-zA-Z0-9\.\_-]+)',0)
and plat in (1, 2)
group by muid,
         trim(packagename),
         trim(apppkg),
         if(appkey is null
            or trim(regexp_replace(appkey,'\"','')) in ('null','NULL')
            or trim(regexp_replace(appkey,'\"','')) not rlike '^[0-9a-fA-FmM]{0,40}$',
            '',trim(regexp_replace(appkey,'\"',''))
         ),
         if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)),
         trim(clientip),
         plat;
"
