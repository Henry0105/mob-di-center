#!/bin/sh

set -e -x
#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

# input
device_unstall_1month=${dm_mobdi_tmp}.device_unstall_1month
device_unstall_2month=${dm_mobdi_tmp}.device_unstall_2month
device_unstall_3month=${dm_mobdi_tmp}.device_unstall_3month
# output
device_unstall_freq_risk=${dm_mobdi_tmp}.device_unstall_freq_risk

hive -e"
insert overwrite  table $device_unstall_freq_risk
select device, avg(pkg_unstall_freq_risk) as unstall_freq_risk 
from 
(
  select device, cnt_pkg, cnt_all, ln(pkg_unstall_freq)/(ln(pkg_unstall_freq) + 1) as pkg_unstall_freq_risk
  from 
  (
    select device, cnt_pkg, cnt_all, cnt_all/cnt_pkg as pkg_unstall_freq
    from $device_unstall_1month
  ) as a 
  union all 
  select device, cnt_pkg, cnt_all, ln(pkg_unstall_freq)/(ln(pkg_unstall_freq) + 1) as pkg_unstall_freq_risk
  from 
  (
    select device, cnt_pkg, cnt_all, cnt_all/cnt_pkg as pkg_unstall_freq
    from $device_unstall_2month
  ) as b 
  union all 
  select device, cnt_pkg, cnt_all, ln(pkg_unstall_freq)/(ln(pkg_unstall_freq) + 1) as pkg_unstall_freq_risk
  from 
  (
    select device, cnt_pkg, cnt_all, cnt_all/cnt_pkg as pkg_unstall_freq
    from $device_unstall_3month
  ) as c 
) as d 
group by device
"