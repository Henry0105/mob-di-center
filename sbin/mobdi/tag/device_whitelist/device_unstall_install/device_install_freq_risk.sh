#!/bin/sh

set -e -x

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp

# input
device_install_1month=$tmpdb.device_install_1month
device_install_2month=$tmpdb.device_install_2month
device_install_3month=$tmpdb.device_install_3month
# output
device_install_freq_risk=$tmpdb.device_install_freq_risk



hive -e"
insert overwrite table $device_install_freq_risk
select device, avg(pkg_install_freq_risk) as install_freq_risk 
from 
(
  select device, cnt_pkg, cnt_all, ln(pkg_install_freq)/(ln(pkg_install_freq) + 1) as pkg_install_freq_risk
  from 
  (
    select device, cnt_pkg, cnt_all, cnt_all/cnt_pkg as pkg_install_freq
    from $device_install_1month
  ) as a 
  union all 
  select device, cnt_pkg, cnt_all, ln(pkg_install_freq)/(ln(pkg_install_freq) + 1) as pkg_install_freq_risk
  from 
  (
    select device, cnt_pkg, cnt_all, cnt_all/cnt_pkg as pkg_install_freq
    from $device_install_2month
  ) as b 
  union all 
  select device, cnt_pkg, cnt_all, ln(pkg_install_freq)/(ln(pkg_install_freq) + 1) as pkg_install_freq_risk
  from 
  (
    select device, cnt_pkg, cnt_all, cnt_all/cnt_pkg as pkg_install_freq
    from $device_install_3month
  ) as c 
) as d 
group by device
"