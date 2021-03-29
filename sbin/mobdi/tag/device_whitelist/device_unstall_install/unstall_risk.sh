#!/bin/sh

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties


# input
device_unstall_1month=${dm_mobdi_tmp}.device_unstall_1month
device_unstall_2month=${dm_mobdi_tmp}.device_unstall_2month
device_unstall_3month=${dm_mobdi_tmp}.device_unstall_3month
# output
device_unstall_risk=${dm_mobdi_tmp}.device_unstall_risk

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

insert overwrite table $device_unstall_risk
select device, avg(risk) as unstall_risk 
from 
(
  select device, cnt_pkg_risk as risk
  from 
  (
    select device, cnt_pkg, cnt_all, 
    case 
      when cnt_pkg <= (8+1.5*(8-2)) then 0
      when cnt_pkg > (8+1.5*(8-2)) and cnt_pkg <= 30 then cnt_pkg*1/(30-(8+1.5*(8-2))) + (1 - 30*1/(30-(8+1.5*(8-2))))
      when cnt_pkg > 30 then 1
    end as cnt_pkg_risk
    from 
    (
      select device, cnt_pkg, cnt_all
      from $device_unstall_1month
    ) as m1 
  ) as a 
  union all 
  select device, cnt_pkg_risk as risk
  from 
  (
    select device, cnt_pkg, cnt_all, 
    case 
      when cnt_pkg <= (12+1.5*(12-2)) then 0
      when cnt_pkg > (12+1.5*(12-2)) and cnt_pkg <= 40 then cnt_pkg*1/(40-(12+1.5*(12-2))) + (1 - 40*1/(40-(12+1.5*(12-2))))
      when cnt_pkg > 40 then 1
    end as cnt_pkg_risk
    from 
    (
      select device, cnt_pkg, cnt_all
      from $device_unstall_2month
    ) as m2 
  ) as b 
  union all 
  select device, cnt_pkg_risk as risk
  from 
  (
    select device, cnt_pkg, cnt_all, 
    case 
      when cnt_pkg <= (15+1.5*(15-3)) then 0
      when cnt_pkg > (15+1.5*(15-3)) and cnt_pkg <= 50 then cnt_pkg*1/(50-(15+1.5*(15-3))) + (1 - 50*1/(50-(15+1.5*(15-3))))
      when cnt_pkg > 50 then 1
    end as cnt_pkg_risk
    from 
    (
      select device, cnt_pkg, cnt_all
      from $device_unstall_3month
    ) as m3 
  ) as c 
) as d 
group by device
"