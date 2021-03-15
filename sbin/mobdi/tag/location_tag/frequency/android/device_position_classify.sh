#!/bin/sh

set -x -e

: '
@owner:zhoup
@describe:计算常去地是城市还是乡村
@projectName:MobDI
'

#入参
day=$1
date=${day:0:6}01

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#input
#rp_device_frequency_3monthly=dm_mobdi_report.rp_device_frequency_3monthly

#mapping
#chinese_area_code_new=dm_sdk_mapping.chinese_area_code_new

#output
#ads_device_frequencry_position_classify=dm_mobdi_report.ads_device_frequencry_position_classify

hive -e"
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

with frequency_info as(
  select device,
         cnt,
         case 
           when substring(area,length(area),1) in ('区','市','岛') then 1 
           when substring(area,length(area),1) in ('县','旗') then 2
         else 0 end as flag
  from
  (
    select frequency.device,
           frequency.cnt,
           area_mapping.area
    from
    (
      select device,
             area,
             cnt
      from $rp_device_frequency_3monthly
      where day='$date'
      and length(trim(area))>0
    ) frequency
    inner join $chinese_area_code_new area_mapping
    on frequency.area = area_mapping.area_code
  )t
)

insert overwrite table $ads_device_frequencry_position_classify partition(day =$date)
select device,
       case
         when sum(unben_sum) >sum(rural_sum) then 1
         when sum(unben_sum) <sum(rural_sum) then 2
         else 3
       end as type
from
(
  select 
    device,
    case when flag=1 then sum(cnt) else 0 end as  unben_sum,
    case when flag=2 then sum(cnt) else 0 end as  rural_sum
  from frequency_info
  where flag <> 0
  group by device, flag
)tt
group by device;
"
