#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

day=$1
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
sql="
insert overwrite table ${device_applist_new} partition(day=$day)
select device,COALESCE(b.apppkg,a.pkg) as pkg,$day as processtime 
from 
(
  select device,pkg
  from ${dws_device_install_app_re_status_di}
  where day = $day
  and refine_final_flag <> -1
) a
left join
(
  select *
  from dm_sdk_mapping.app_pkg_mapping_par
  where version='1000'
  and apppkg <> ''
  and apppkg is not null
) b on (a.pkg = b.pkg)
group by device,COALESCE(b.apppkg,a.pkg);
"
echo "$sql"

hive -v -e "$sql"
