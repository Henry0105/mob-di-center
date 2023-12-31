#!/bin/sh
set -e -x
: '
@owner:zhtli
@describe:计算设备第一次入网时间
@projectName:MobDI
@BusinessName:device_mintime
@SourceTable:dw_mobdi_etl.log_device_info_jh,dw_mobdi_etl.log_device_info,rp_mobdi_app.device_mintime_mapping,rp_mobdi_app.device_mintime_incr_mapping
@TargetTable:rp_mobdi_app.device_mintime_incr_mapping,rp_mobdi_app.device_mintime_mapping
@TableRelation:dw_mobdi_etl.log_device_info_jh,dw_mobdi_etl.log_device_info,rp_mobdi_app.device_mintime_mapping->rp_mobdi_app.device_mintime_incr_mapping|rp_mobdi_app.device_mintime_incr_mapping->rp_mobdi_app.device_mintime_mapping
'

day=$1
if [ $# -lt 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>"
    exit 1
fi

: '
实现步骤: 1.dw_mobdi_etl.log_device_info_jh与dw_mobdi_etl.log_device_info都取当日分区的设备和平台字段数据, 合并在一起去重
          2.找出步骤2所有不在rp_mobdi_app.device_mintime_mapping中的数据, 结果存入rp_mobdi_app.device_mintime_incr_mapping
          3.把rp_mobdi_app.device_mintime_incr_mapping中的数据并入到全量表rp_mobdi_app.device_mintime_mapping中
'

source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

#out
#device_mintime_incr_mapping=$device_mintime_incr_mapping
#device_mintime_mapping=$device_mintime_mapping

hive -v -e"
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts='-Xmx4600m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx4600m';
set mapreduce.reduce.memory.mb=5120;
set mapreduce.reduce.java.opts='-Xmx4600m' -XX:+UseG1GC;

set hive.optimize.index.filter=true;
set hive.exec.orc.zerocopy=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.ppd=true;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=250000000;
set mapred.min.split.size.per.rack=250000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.exec.reducers.bytes.per.reducer=256000000;

set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;

insert overwrite table $device_mintime_incr_mapping partition(day='${day}')
select incr.device, incr.plat
from
(
  select device,plat
  from
  (
      select device,plat
      from $dwd_log_device_info_jh_sec_di
      where day = '${day}'
      and plat in ('1','2')
      group by device,plat

      union all

      select device,1 as plat
      from $dim_device_applist_new_di
      where day = '${day}'
      group by device

      union all

      select device,plat
      from $dws_device_ip_info_di
      where day = '${day}'
      and plat in (1,2)
      group by device,plat
  )a
  group by device,plat
) incr
left join
$device_mintime_mapping ful
on incr.device = ful.device
where ful.device is null;

insert overwrite table $device_mintime_mapping
select device, plat, day
from $device_mintime_mapping

union all

select device, plat, day
from $device_mintime_incr_mapping
where day='${day}';
"
