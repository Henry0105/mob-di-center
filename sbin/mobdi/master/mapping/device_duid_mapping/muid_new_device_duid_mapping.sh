#!/bin/bash
: '
@owner: menff
@describe:
@projectName:
@BusinessName:
@SourceTable:dm_sdk_mapping.device_duid_mapping_new,dw_mobdi_etl.log_device_info_jh
@TargetTable:dm_sdk_mapping.device_duid_mapping_new
@TableRelation:dw_mobdi_etl.log_device_info_jh,dw_mobdi_etl.pv,dw_mobdi_etl.dcookie->dm_sdk_mapping.device_duid_mapping_new
'

: '
1. 内部id_mapping:
   source表: dw_mobdi_etl.log_device_info_jh,dw_mobdi_etl.pv, dw_mobdi_etl.dcookie
主要逻辑: 
1. 主键: device,duid,dcookie,plat
2. 
安卓: 从dw_mobdi_etl.log_device_info_jh (plat=1),dw_mobdi_etl.pv (plat=1)中取device,duid上去重,1 as plat, dcookie置为空
ios: 从dw_mobdi_etl.log_device_info_jh (plat=2),dw_mobdi_etl.pv (plat=2)中取device,duid,dcookie置为空, dw_mobdi_etl.dcookie 中取device,duid,dcookie ;2 as plat; device,duid,dcookie上进行去重
3. 保留一个processtime记录device,duid,plat,dcookie最早出现的时间
4. device,duid,dcookie若不符合清洗条件 (由字母和数字组成) 统一置为空

表更新频率: 每天
表更新方式: 全量表,全量表每个月月初备份一张表
'

currentDay=$1

# input
log_device_info_jh=dw_mobdi_etl.log_device_info_jh
pv=dw_mobdi_etl.pv
dcookie=dw_mobdi_etl.dcookie

# output   名字还要修改,  dm_mobdi_topic.dws_device_duid_mapping_new  ?
dws_device_duid_mapping_new=dm_mobdi_topic.dws_device_duid_mapping_new



hive -v -e "
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.map.aggr=true;
set hive.auto.convert.join=true;
set hive.groupby.skewindata=true;

insert overwrite table $dws_device_duid_mapping_new
select device, duid, plat, dcookie, processtime
from
(
  select device, duid, plat, dcookie, processtime,
         row_number() over (partition by device, duid, plat order by processtime asc, dcookie desc) rn
  from
  (
    select device, duid, plat, dcookie, processtime
    from $dws_device_duid_mapping_new
    where length(duid) > 0

    union all

    select device, duid, plat, dcookie, processtime
    from
    (
      select lower(trim(muid)) as device,
             coalesce(if(lower(trim(curduid)) < 0, '', lower(trim(curduid))), lower(trim(id))) as duid,
             plat,
             '' as dcookie,
             processtime
      from $log_device_info_jh
      where dt=$currentDay
      and plat in (1, 2)

      union all

      select lower(trim(muid)) as device,
             lower(trim(duid)) as duid,
             plat,
             '' as dcookie,
             day as processtime
      from $pv
      where day=$currentDay and plat in (1, 2)

      union all

      select lower(trim(muid)) as device,
             lower(trim(id)) as duid,
             2 as plat,
             lower(trim(duidcookie)) as dcookie,
             day as processtime
      from $dcookie
      where day=$currentDay
    ) as a
    where length(duid) > 0
    group by device, duid, plat, dcookie, processtime
  ) as b
) as c
where rn = 1
"