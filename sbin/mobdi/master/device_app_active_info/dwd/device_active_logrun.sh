#!/bin/bash
: '
@owner: liuyanqiang
@describe: 设备的app日活跃明细表
@projectName:MobDI
@BusinessName:设备的app日活跃明细表-log_run_new
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
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di

## 目标表
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

HADOOP_USER_NAME=dba hive -v -e "
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=1000;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts=-Xmx6g -XX:+UseG1GC;
SET mapreduce.reduce.memory.mb=10240;
SET mapreduce.reduce.java.opts='-Xmx8g';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $dws_device_active_di PARTITION (day = '$day', plat, source)
select if(plat=1,muid,deviceid) as device,
       trim(apppkg) as pkg,
       trim(apppkg) AS apppkg,
       if(appkey is null
          or trim(regexp_replace(appkey,'\"','')) in ('null','NULL')
          or trim(regexp_replace(appkey,'\"','')) not rlike '^[0-9a-fA-FmM]{0,40}$',
          '',trim(regexp_replace(appkey,'\"',''))
       ) as appkey,
       if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)) as appver,
       trim(clientip) as ip,
       '' as commonsdkver,
       sort_array(array(map(
         'k', 'SHARESDK',
         'v', if(sdkver is null or sdkver not rlike '^[0-9.]+$','',trim(sdkver))
       ))) as sdks,
       0 as tot_times,0 as active_cnt,0 as front_active_cnt,0 as back_active_cnt,
       concat_ws(',',sort_array(collect_list(from_unixtime(cast(substring(clienttime,1,10) as bigint),'yyyy-MM-dd HH:mm:ss')))) as clienttime_list,
       concat_ws(',',sort_array(collect_list(servertime))) as servertime_list,
       plat,
       'logrun' as source
from $dwd_log_run_new_di
where day = '$day'
and trim(lower(if(plat=1,muid,deviceid))) rlike '^[a-f0-9]{40}$'
and trim(if(plat=1,muid,deviceid)) != '0000000000000000000000000000000000000000'
and apppkg is not null
and trim(apppkg) not in ('','null','NULL')
and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
and plat in (1, 2)
group by if(plat=1,muid,deviceid),
         trim(apppkg),
         if(appkey is null
            or trim(regexp_replace(appkey,'\"','')) in ('null','NULL')
            or trim(regexp_replace(appkey,'\"','')) not rlike '^[0-9a-fA-FmM]{0,40}$',
            '',trim(regexp_replace(appkey,'\"',''))
         ),
         if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)),
         trim(clientip),
         sort_array(array(map(
           'k', 'SHARESDK',
           'v', if(sdkver is null or sdkver not rlike '^[0-9.]+$','',trim(sdkver))
         ))),
         plat;
"
