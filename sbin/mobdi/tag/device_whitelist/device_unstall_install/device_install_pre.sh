#!/bin/sh

set -e -x

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp

# input
unstall_install_risk_pre=$tmpdb.unstall_install_risk_pre
# output
device_install_1month=$tmpdb.device_install_1month
device_install_2month=$tmpdb.device_install_2month
device_install_3month=$tmpdb.device_install_3month

day=$1

p1months=`date -d "$day -30 days" +%Y%m%d`
p2months=`date -d "$day -60 days" +%Y%m%d`
p3months=`date -d "$day -90 days" +%Y%m%d`

hive -e"
insert overwrite table $device_install_1month
  select device, cnt_pkg, cnt_all 
  from 
  (
    select device, count(*) over(partition by device) as cnt_pkg, sum(cnt) over(partition by device) as cnt_all
    from 
    (
      select device, pkg, count(*) as cnt
    from $unstall_install_risk_pre
    where day between '$p1months' and '$day' and refine_final_flag = 1
      group by device, pkg
    ) as b 
  ) as c 
  group by device, cnt_pkg, cnt_all
"


hive -e"
insert overwrite table $device_install_2month
  select device, cnt_pkg, cnt_all 
  from 
  (
    select device, count(*) over(partition by device) as cnt_pkg, sum(cnt) over(partition by device) as cnt_all 
    from 
    (
      select device, pkg, count(*) as cnt
      from $unstall_install_risk_pre
      where day between '$p2months' and '$day' and refine_final_flag = 1
      group by device, pkg
    ) as b 
  ) as c 
  group by device, cnt_pkg, cnt_all
"

hive -e"
insert overwrite table $device_install_3month
  select device, cnt_pkg, cnt_all 
  from 
  (
    select device, count(*) over(partition by device) as cnt_pkg, sum(cnt) over(partition by device) as cnt_all 
    from 
    (
      select device, pkg, count(*) as cnt
      from $unstall_install_risk_pre
      where day between '$p3months' and '$day' and refine_final_flag = 1
      group by device, pkg
    ) as b 
  ) as c 
  group by device, cnt_pkg, cnt_all
"

