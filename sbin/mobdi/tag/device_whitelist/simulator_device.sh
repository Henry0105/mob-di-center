#!/bin/sh

set -e -x

day=$1

p1month=`date -d "$day -30 days" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh

# input
#dwd_simulator_det_info_sec_di=dm_mobdi_master.dwd_simulator_det_info_sec_di
# output
device_simulator_full=${dm_mobdi_tmp}.device_simulator_full

hive -e"
insert overwrite table $device_simulator_full
select device ,model
from(
  select device ,model from $device_simulator_full
  union all
  select muid as device, model
  from $dwd_simulator_det_info_sec_di
  where day >= '$p1month' and day <= '$day' and qemukernel = '1'
)t
group by device,model
"