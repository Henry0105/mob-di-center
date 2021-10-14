#!/bin/sh

set -e -x

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

# input
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par

# output
unstall_install_risk_pre=${dm_mobdi_tmp}.unstall_install_risk_pre

day=$1
p1month=`date -d "$day -30 days" +%Y%m%d`
p3month=`date -d "$day -90 days" +%Y%m%d`

hive -e"
insert overwrite table $unstall_install_risk_pre
select m.device, coalesce(n.apppkg, m.pkg) as pkg, m.day,refine_final_flag
      from 
      (
        select device, pkg, day,refine_final_flag
        from $dws_device_install_app_re_status_di a
        left semi join
        (select device from $dws_device_install_app_re_status_di where day >= '$p1month' and day <= '$day'
        and pkg is not null and trim(pkg) <> '' group by device) b
        on a.device=b.device
        where day >= '$p3month' and day <= '$day'
        and pkg is not null and trim(pkg) <> ''
      ) as m 
      left join 
      (
        select pkg, apppkg
        from $dim_app_pkg_mapping_par
        where version = '${day}.1000'
        group by pkg, apppkg
      ) as n
      on m.pkg = n.pkg
      group by m.device, coalesce(n.apppkg, m.pkg), m.day,refine_final_flag
"
