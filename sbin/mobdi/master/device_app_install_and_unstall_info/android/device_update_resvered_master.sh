#! /bin/sh
set -e -x

: '
@owner:xdzhang
@describe:对于没有捕捉到的安装和卸载做一些修正，同时修正由于升级捕捉到的安装
@projectName:
@BusinessName:
'


if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1
bdate1=`date -d "$day -40 days" "+%Y%m%d"`

HADOOP_USER_NAME=dba hive -v -e"
SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=root.yarn_data_compliance;

insert overwrite table dm_mobdi_topic.dws_device_install_app_re_status_di partition(day='${day}')
select device,pkg,
       reserved_flag as refine_final_flag,
       if(install_day='$day',1,0) as install_flag,
       if(unstall_day='$day',1,0) as unstall_flag,
       final_flag
from
(
  select device,
         pkg,
         from_unixtime(cast(install_datetime/1000 as int),'yyyyMMdd') as install_day,
         from_unixtime(cast(unstall_datetime/1000 as int),'yyyyMMdd') as unstall_day,
         final_flag,
         reserved_flag
  from dm_mobdi_topic.dws_device_install_status
  where day = '${day}'
  and process_time = '$day'
  and (final_flag != -1 or from_unixtime(cast(unstall_datetime/1000 as int),'yyyyMMdd')='$day')
) t1;
"
