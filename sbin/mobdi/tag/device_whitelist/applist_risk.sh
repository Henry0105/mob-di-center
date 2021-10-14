#!/bin/sh

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
# input
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
# output
device_applist_risk=${dm_mobdi_tmp}.device_applist_risk

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with applist_all as(
select device, day, count(*) as cnt 
from 
(
  select m.device, coalesce(n.apppkg, m.pkg) as pkg, m.day
  from 
  (
    select device, pkg, day
    from $dws_device_install_app_status_40d_di
    where day = '$day' and final_flag <> -1
  ) as m 
  left join 
  (
    select pkg, apppkg
    from $dim_app_pkg_mapping_par
    where version = '${day}.1000'
    group by pkg, apppkg
  ) as n
  on m.pkg = n.pkg
  group by m.device, coalesce(n.apppkg, m.pkg), m.day
) as a 
group by device, day
)
insert overwrite table $device_applist_risk
select device, day, cnt, 
case 
  when cnt < 6 then 0.4
  when cnt >= 6 and cnt <= 253 then cnt*(0.2-0)/(253-5) + (0.2-253*(0.2-0)/(253-5))
  when cnt > 253 and cnt <= 656 then cnt*(1-0.2)/(656-253) + (1-656*(1-0.2)/(656-253))
  when cnt > 656 then 1
end as app_risk
from 
(
  select device, day, cnt
  from applist_all
) as a
"