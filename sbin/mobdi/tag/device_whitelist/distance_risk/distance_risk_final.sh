#!/bin/sh

set -x -e
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

# input
device_distance_risk_1month=${dm_mobdi_tmp}.device_distance_risk_1month
device_distance_risk_2month=${dm_mobdi_tmp}.device_distance_risk_2month
device_distance_risk_3month=${dm_mobdi_tmp}.device_distance_risk_3month
# output
device_distance_risk=${dm_mobdi_tmp}.device_distance_risk

hive -e"
insert overwrite table $device_distance_risk
select device, avg(risk) as distance_risk
from 
(
  select device, distance_risk_1month as risk
  from $device_distance_risk_1month
  union all 
  select device, distance_risk_2month as risk
  from $device_distance_risk_2month
  union all 
  select device, distance_risk_3month as risk
  from $device_distance_risk_3month
) as a 
group by device
"