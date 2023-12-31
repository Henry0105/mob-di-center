#!/bin/bash
: '
@owner: liuyanqiang
@describe: 设备的app日活跃明细表
@projectName:MobDI
@BusinessName:设备的app日活跃明细表-pv
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
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di

## 目标表
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

HADOOP_USER_NAME=dba hive -v -e  "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function log_pv_sdks_clear as 'com.youzu.mob.java.udf.LogPvSdksClear';
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=1000;
SET mapreduce.map.memory.mb=15360;
set mapreduce.map.java.opts='-Xmx15g';
set mapreduce.reduce.memory.mb=20480;
set mapreduce.reduce.java.opts='-Xmx20480M';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $dws_device_active_di PARTITION (day = '$day', plat, source)
SELECT if(plat=1,muid,deviceid) as device,
       trim(apppkg) as pkg,
       trim(apppkg) as apppkg,
       if(appkey is null
          or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] in ('null','NULL')
          or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] not rlike '^[0-9a-fA-FmM]{0,40}$',
          '',split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0]
       ) as appkey,
       if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)) as appver,
       trim(clientip) as ip,
       if(commonsdkver is null,'',cast(commonsdkver as string)) as commonsdkver,
       sort_array(log_pv_sdks_clear(sdks)) as sdks,
       0 as tot_times,0 as active_cnt,0 as front_active_cnt,0 as back_active_cnt,
       concat_ws(',',sort_array(if(count(1)<=1800000,collect_list(clienttime),collect_set(clienttime)))) as clienttime_list,
       concat_ws(',',sort_array(if(count(1)<=1800000,collect_list(serdatetime),collect_set(serdatetime)))) as servertime_list,
       plat,
       'pv' as source
FROM $dwd_pv_sec_di
WHERE day = '$day'
and trim(lower(if(plat=1,muid,deviceid))) rlike '^[a-f0-9]{40}$'
and trim(if(plat=1,muid,deviceid)) != '0000000000000000000000000000000000000000'
and apppkg is not null
and trim(apppkg) not in ('','null','NULL')
and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
and plat in (1, 2)
GROUP BY if(plat=1,muid,deviceid),
         trim(apppkg),
         if(appkey is null
            or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] in ('null','NULL')
            or split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0] not rlike '^[0-9a-fA-FmM]{0,40}$',
            '',split(trim(regexp_replace(regexp_replace(appkey,'\\\\s+',' '),'\"','')),'\\\\s+')[0]
         ),
         if(appver is null or trim(appver) in ('null','NULL'), '', trim(appver)),
         trim(clientip),
         if(commonsdkver is null,'',cast(commonsdkver as string)),
         sort_array(log_pv_sdks_clear(sdks)),
         plat;
"
