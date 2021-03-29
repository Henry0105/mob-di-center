#!/bin/sh

set -e -x

# input
unstall_install_risk_pre=${dm_mobdi_tmp}.unstall_install_risk_pre
# output
device_unstall_1month=${dm_mobdi_tmp}.device_unstall_1month
device_unstall_2month=${dm_mobdi_tmp}.device_unstall_2month
device_unstall_3month=${dm_mobdi_tmp}.device_unstall_3month

day=$1

p1months=`date -d "$day -30 days" +%Y%m%d`
p2months=`date -d "$day -60 days" +%Y%m%d`
p3months=`date -d "$day -90 days" +%Y%m%d`

hive -e"
insert overwrite table $device_unstall_1month
select device, cnt_pkg, cnt_all 
from 
(
  select device, count(*) over(partition by device) as cnt_pkg, sum(cnt) over(partition by device) as cnt_all
  from 
  (
    select device, pkg, count(*) as cnt
    from $unstall_install_risk_pre
    where day between '$p1months' and '$day' and refine_final_flag = -1
    group by device, pkg
  ) as b 
) as c 
group by device, cnt_pkg, cnt_all
"


hive -e"
insert overwrite table $device_unstall_2month
select device, cnt_pkg, cnt_all 
from 
(
  select device, count(*) over(partition by device) as cnt_pkg, sum(cnt) over(partition by device) as cnt_all 
  from 
  (
    select device, pkg, count(*) as cnt
    from $unstall_install_risk_pre
    where day between '$p2months' and '$day' and refine_final_flag = -1
    group by device, pkg
  ) as b 
) as c 
group by device, cnt_pkg, cnt_all
"


hive -e"
insert overwrite table $device_unstall_3month
select device, cnt_pkg, cnt_all 
from 
(
  select device, count(*) over(partition by device) as cnt_pkg, sum(cnt) over(partition by device) as cnt_all 
  from 
  (
    select device, pkg, count(*) as cnt
    from $unstall_install_risk_pre
    where day between '$p3months' and '$day' and refine_final_flag = -1
    group by device, pkg
  ) as b 
) as c 
group by device, cnt_pkg, cnt_all
"


