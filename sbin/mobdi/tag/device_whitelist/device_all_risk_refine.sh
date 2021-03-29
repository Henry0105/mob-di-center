#!/bin/sh

set -e -x

day=$1
pday=`date -d "$day -30 day" +%Y%m`16

source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
dim_model_blacklist=dim_sdk_mapping.dim_model_blacklist

#tmp
device_info_risk=${dm_mobdi_tmp}.device_info_risk
device_imei_risk_pre=${dm_mobdi_tmp}.device_imei_risk_pre
device_mac_risk_pre=${dm_mobdi_tmp}.device_mac_risk_pre
device_serialno_risk_pre=${dm_mobdi_tmp}.device_serialno_risk_pre
device_all_risk=${dm_mobdi_tmp}.device_all_risk
device_active_risk=${dm_mobdi_tmp}.device_active_risk
device_applist_risk=${dm_mobdi_tmp}.device_applist_risk
device_unstall_risk=${dm_mobdi_tmp}.device_unstall_risk
device_install_risk=${dm_mobdi_tmp}.device_install_risk
device_sharecount_risk=${dm_mobdi_tmp}.device_sharecount_risk
device_oauthcount_risk=${dm_mobdi_tmp}.device_oauthcount_risk
device_bssidcnt_risk=${dm_mobdi_tmp}.device_bssidcnt_risk
device_gps_ip_risk=${dm_mobdi_tmp}.device_gps_ip_risk
device_ip_risk=${dm_mobdi_tmp}.device_ip_risk
device_strange_app_type_install_3month=${dm_mobdi_tmp}.device_strange_app_type_install_3month
device_ip_proxy_p_risk=${dm_mobdi_tmp}.device_ip_proxy_p_risk
device_simulator_full=${dm_mobdi_tmp}.device_simulator_full
#out
device_all_risk_refine=dm_mobdi_report.device_all_risk_refine

hive -e"
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
SET mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with device_info_risk_strongrule as( 
  select device, factory, a.model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, risk as risk_original, flag as flag_original
  from $device_info_risk a
  left semi join
  (select model from $dim_model_blacklist where  day='20190716' )b
  on lower(trim(a.model)) =b.model
),
device_info_risk_strongrule_test as (
  select device, factory, a.model, screensize, public_date, model_type, sysver, breaked, carrier, price, devicetype, processtime, risk as risk_original, flag as flag_original
  from $device_info_risk a
  where lower(trim(a.model)) rlike 'vmos|aosp|arm64|emulator|- api|genymotion|cmvideo|虚拟' 
  and not exists (select model from $dim_model_blacklist b where day='20190716' and  lower(trim(a.model)) = b.model) 
),
device_imei_mac_serialno_strongrule as(
  select device, count(*) as cnt 
  from 
  (
    select device
    from $device_imei_risk_pre
    where imei_risk_all > 1
    union all 
    select device 
    from $device_mac_risk_pre
    where mac_risk_all > 1
    union all 
    select device 
    from $device_serialno_risk_pre
    where serialno_risk_all > 1
  ) as a 
  group by device
),
device_risk_refine_by_strongrule as(
  select device, risk 
  from 
  (
    select device, risk, row_number() over(partition by device order by risk desc) as num
    from 
    (
      select device,risk_refine as risk
      from $device_all_risk_refine
      where day=$pday
      union all
      select device, risk_scale as risk
      from $device_all_risk
      union all
      select device, 90 as risk
      from device_imei_mac_serialno_strongrule
      where cnt = 3
      union all 
      select device, 85 as risk 
      from $device_active_risk
      where active_risk >= 0.999
      union all 
      select device, 85 as risk 
      from $device_applist_risk
      where app_risk = 1
      union all 
      select device, 70 as risk
      from $device_unstall_risk
      where unstall_risk = 1
      union all 
      select device, 70 as risk
      from $device_install_risk
      where install_risk = 1
      union all 
      select device, 60 as risk
      from $device_sharecount_risk
      where share_risk = 1
      union all 
      select device, 60 as risk
      from $device_oauthcount_risk
      where oauth_risk = 1
      union all 
      select device, 60 as risk
      from $device_bssidcnt_risk
      where bssidcnt_risk = 1
      union all  
      select device, 90 as risk
      from $device_gps_ip_risk
      where gps_ip_vs_risk >= 0.9
      union all 
      select device, 70 as risk
      from $device_ip_risk
      where ip_risk = 1
      union all 
      select device, 100 as risk
      from $device_strange_app_type_install_3month
      union all 
      select device, 100 as risk
      from $device_ip_proxy_p_risk
      where daili_ip_p_risk >= 0.9
      union all 
      select device, 100 as risk 
      from $device_simulator_full
      union all 
      select device, 100 as risk 
      from device_info_risk_strongrule
      union all 
      select device, 100 as risk 
      from device_info_risk_strongrule_test
    ) as a 
  ) as b 
  where num = 1
)
insert overwrite table $device_all_risk_refine partition(day=$day)
select a.device, a.imei_risk, a.mac_risk, a.serialno_risk, a.model_risk, a.active_risk, a.share_risk, a.oauth_risk, a.app_risk, a.unstall_risk, a.install_risk, a.strange_app_risk, a.bssidcnt_risk, a.network_risk, a.distance_risk, a.gps_ip_vs_risk, a.ip_risk, a.daili_ip_risk as proxy_ip_risk, a.unstall_freq_risk, a.install_freq_risk, a.ip_shang_risk, a.daili_ip_p_risk as proxy_ip_p_risk, a.risk_all, a.risk_scale, b.risk as risk_refine, a.null_cnt, a.ifhaverisk
from $device_all_risk as a 
left join device_risk_refine_by_strongrule as b 
on a.device = b.device
;
"