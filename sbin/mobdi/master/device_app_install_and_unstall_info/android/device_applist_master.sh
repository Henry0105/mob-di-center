#! /bin/sh 

set  -x -e

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

#入参
day=$1
day40=`date -d "$day -40 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

## 源表
#dws_device_install_status=dm_mobdi_topic.dws_device_install_status
#dws_device_sdk_run_master_di=dm_mobdi_topic.dws_device_sdk_run_master_di

## 目标表
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di

#逻辑分为两块
#第一块，取近40天的dws_device_install_status在装设备app信息
#第二块，取近40天的活跃设备，与dws_device_install_status表41天到180天的在装设备数据进行关联
#超过180天的数据，表示已经180天没有获取到设备的任何在装信息了，就算最近有活跃，也不参与在装计算
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
SET mapreduce.map.memory.mb=10240;
set mapreduce.map.java.opts='-Xmx10g';
set mapreduce.child.map.java.opts='-Xmx10g';
set mapreduce.reduce.memory.mb=10240;
set mapreduce.reduce.java.opts='-Xmx10240M';
set mapreduce.child.reduce.java.opts='-Xmx10240M';
insert overwrite table $dws_device_install_app_status_40d_di partition(day='$day')
select process_time,
       device,
       pkg,
       if(install_day='$day',1,0) as install_flag,
       if(unstall_day='$day',1,0) as unstall_flag,
       install_datetime,
       unstall_datetime,
       final_flag,
       active_time
from
(
  select device,
         pkg,
         from_unixtime(cast(install_datetime/1000 as int),'yyyyMMdd') as install_day,
         from_unixtime(cast(unstall_datetime/1000 as int),'yyyyMMdd') as unstall_day,
         if(install_datetime='0','0',from_unixtime(cast(install_datetime/1000 as int),'yyyy-MM-dd HH:mm:ss')) as install_datetime,
         if(unstall_datetime='0','0',from_unixtime(cast(unstall_datetime/1000 as int),'yyyy-MM-dd HH:mm:ss')) as unstall_datetime,
         final_flag,
         process_time,
         process_time as active_time
  from $dws_device_install_status
  where day='${day}'
  and process_time<='$day'
  and process_time>'$day40'

  union all

  select t1.device,
         pkg,
         '' as install_day,
         '' as unstall_day,
         if(install_datetime='0','0',from_unixtime(cast(install_datetime/1000 as int),'yyyy-MM-dd HH:mm:ss')) as install_datetime,
         if(unstall_datetime='0','0',from_unixtime(cast(unstall_datetime/1000 as int),'yyyy-MM-dd HH:mm:ss')) as unstall_datetime,
         final_flag,
         process_time,
         t2.active_time
  from
  (
    select device,pkg,install_datetime,unstall_datetime,final_flag,process_time
    from $dws_device_install_status
    where day='${day}'
    and process_time<='$day40'
  ) t1
  inner join
  (
    select device,max(day) as active_time
    from $dws_device_sdk_run_master_di
    where day<='$day'
    and day>'$day40'
    group by device
  ) t2 on t1.device=t2.device
) t1
"
