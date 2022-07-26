#!/bin/bash
: '
@owner: qinff
@describe:
@projectName:
@BusinessName:
@SourceTable:dm_mobdi_master.dwd_device_location_info_di
@TargetTable:dm_mobdi_master.dwd_device_location_info_di_v2
'
## dwd_device_location_info_di_v2表对dwd_device_location_info_di数据做了重分布，去掉data_source分区，添加国内的按照省份做分区，国外在一个分区abord


set -x -e

if [ -z "$1" ]; then
  exit 1
fi

day=$1


source /home/dba/mobdi_center/conf/hive-env.sh


###源表
#dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di

###目标表
#dwd_device_location_info_di_v2=dm_mobdi_master.dwd_device_location_info_di_v2

#set dfs.replication=3;
#set mapreduce.job.queuename=${queue};
hive -S -v -e "
set mapred.job.name='insert ${dwd_device_location_info_di_v2} day=${day}';
set mapred.min.split.size.per.node=32000000;
set mapred.min.split.size.per.rack=32000000;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=32000000;
set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.type=block;
set hive.exec.compress.output= true;
set mapred.output.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.created.files=10000;
set hive.exec.max.dynamic.partitions.pernode=200;
set hive.exec.default.partition.name='cn_unknow_province';

set yarn.scheduler.minimum-allocation-mb=1024;
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts='-Xmx1840m';
set mapreduce.child.map.java.opts='-Xmx1840m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set mapred.job.reuse.jvm.num.tasks=10;
set mapred.tasktracker.map.tasks.maximum=24;
set mapred.tasktracker.reduce.tasks.maximum=24;

set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.exec.reducers.bytes.per.reducer=3221225472;

set hive.optimize.index.filter=true;
set hive.exec.orc.split.strategy=BI;
set hive.mapred.reduce.tasks.speculative.execution=false;
set mapreduce.map.speculative=false;
set mapreduce.reduce.speculative=false;
set mapreduce.job.reduce.slowstart.completedmaps=0.9;


insert overwrite table ${dwd_device_location_info_di_v2} partition(day='${day}',province_cn)
select device,
       duid,
       lat,
       lon,
       time,
       processtime,
       country,
       province,
       city,
       area,
       street,
       plat,
       network,
       type,
       data_source,
       orig_note1,
       orig_note2,
       accuracy,
       apppkg,
       orig_note3,
       abnormal_flag,
       ga_abnormal_flag,
       case  when country='cn' and province is not null and province <>''
               then province
             when country='cn' and ( province ='' or province is null )
               then 'cn_unknow_province'
             else 'abroad'
        end as province_cn
from ${dwd_device_location_info_di} where day='${day}'
distribute by device sort by city asc;
exit;"
