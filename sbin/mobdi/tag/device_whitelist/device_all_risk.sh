#!/bin/sh

set -e -x

day=$1

source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#input
#dim_id_mapping_android_sec_df_view=dim_mobdi_mapping.dim_id_mapping_android_sec_df_view
#tmp
device_all_risk_pre=${dm_mobdi_tmp}.device_all_risk_pre
device_info_risk=${dm_mobdi_tmp}.device_info_risk
device_active_risk=${dm_mobdi_tmp}.device_active_risk
device_sharecount_risk=${dm_mobdi_tmp}.device_sharecount_risk
device_oauthcount_risk=${dm_mobdi_tmp}.device_oauthcount_risk
device_applist_risk=${dm_mobdi_tmp}.device_applist_risk
device_unstall_risk=${dm_mobdi_tmp}.device_unstall_risk
device_install_risk=${dm_mobdi_tmp}.device_install_risk
device_strange_app_type_install_3month=${dm_mobdi_tmp}.device_strange_app_type_install_3month
device_bssidcnt_risk=${dm_mobdi_tmp}.device_bssidcnt_risk
device_network_risk=${dm_mobdi_tmp}.device_network_risk
device_distance_risk=${dm_mobdi_tmp}.device_distance_risk
device_gps_ip_risk=${dm_mobdi_tmp}.device_gps_ip_risk
device_ip_risk=${dm_mobdi_tmp}.device_ip_risk
device_ip_proxy_3month=${dm_mobdi_tmp}.device_ip_proxy_3month
device_unstall_freq_risk=${dm_mobdi_tmp}.device_unstall_freq_risk
device_install_freq_risk=${dm_mobdi_tmp}.device_install_freq_risk
device_ip_entropy_risk=${dm_mobdi_tmp}.device_ip_entropy_risk
device_ip_proxy_p_risk=${dm_mobdi_tmp}.device_ip_proxy_p_risk
#out
device_all_risk=${dm_mobdi_tmp}.device_all_risk

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

insert overwrite table $device_all_risk_pre
select device, imei_risk, mac_risk, serialno_risk, model_risk, active_risk, share_risk, oauth_risk, app_risk, unstall_risk, install_risk, strange_app_risk, bssidcnt_risk, network_risk, distance_risk, gps_ip_vs_risk, ip_risk, daili_ip_risk, unstall_freq_risk, install_freq_risk, ip_shang_risk, daili_ip_p_risk, floor(risk/1000) as null_cnt,risk_n*15/(15 - floor(risk/1000)) as risk_fangda_scale,ifhaverisk
from
(
  select device, imei_risk, mac_risk, serialno_risk, model_risk, active_risk, share_risk, oauth_risk, app_risk, unstall_risk, install_risk, strange_app_risk, bssidcnt_risk, network_risk, distance_risk, gps_ip_vs_risk, ip_risk, daili_ip_risk, unstall_freq_risk, install_freq_risk, ip_shang_risk, daili_ip_p_risk, (imei_risk_p + mac_risk_p + serialno_risk_p + model_risk_p + active_risk_p + share_risk_p + oauth_risk_p + app_risk_p + unstall_risk_p + install_risk_p + strange_app_risk_n + bssidcnt_risk_p + network_risk_p + distance_risk_p + gps_ip_vs_risk_p + ip_risk_p + daili_ip_risk_n + unstall_freq_risk_n + install_freq_risk_n + ip_shang_risk_n + daili_ip_p_risk_n) as risk,(imei_risk_n + mac_risk_n  + serialno_risk_n + 1.5*model_risk_n + 1.5*active_risk_n + share_risk_n + oauth_risk_n + 1.5*app_risk_n + unstall_risk_n + install_risk_n + 2*strange_app_risk_n + bssidcnt_risk_n + 0.5*network_risk_n + 0.3*distance_risk_n + gps_ip_vs_risk_n + ip_risk_n + 0.3*daili_ip_risk_n + unstall_freq_risk_n + install_freq_risk_n + ip_shang_risk_n + 2*daili_ip_p_risk_n) as risk_n,ifhaverisk
  from
  (
    select a.device,
    device_all_risk.imei_risk,
    nvl(device_all_risk.imei_risk,1000) as imei_risk_p,
    nvl(device_all_risk.imei_risk,0) as imei_risk_n,
    device_all_risk.mac_risk,
    nvl(device_all_risk.mac_risk,1000) as mac_risk_p,
    nvl(device_all_risk.mac_risk,0) as mac_risk_n,
    device_all_risk.serialno_risk,
    nvl(device_all_risk.serialno_risk,1000) as serialno_risk_p,
    nvl(device_all_risk.serialno_risk,0) as serialno_risk_n,
    nvl(e.model_risk,device_all_risk.model_risk) as model_risk,
    coalesce(e.model_risk,device_all_risk.model_risk,1000) as model_risk_p,
    coalesce(e.model_risk,device_all_risk.model_risk,0) as model_risk_n,
    nvl(f.active_risk,device_all_risk.active_risk) as active_risk,
    coalesce(f.active_risk,device_all_risk.active_risk,1000) as active_risk_p,
    coalesce(f.active_risk,device_all_risk.active_risk,0) as active_risk_n,
    nvl(g.share_risk,device_all_risk.share_risk) as share_risk,
    coalesce(g.share_risk,device_all_risk.share_risk,1000) as share_risk_p,
    coalesce(g.share_risk,device_all_risk.share_risk,0) as share_risk_n,
    nvl(h.oauth_risk,device_all_risk.oauth_risk) as oauth_risk,
    coalesce(h.oauth_risk,device_all_risk.oauth_risk,1000) as oauth_risk_p,
    coalesce(h.oauth_risk,device_all_risk.oauth_risk,0) as oauth_risk_n,
    nvl(i.app_risk,device_all_risk.app_risk) as app_risk,
    coalesce(i.app_risk,device_all_risk.app_risk,1000) as app_risk_p,
    coalesce(i.app_risk,device_all_risk.app_risk,0) as app_risk_n,
    nvl(j.unstall_risk,device_all_risk.unstall_risk) as unstall_risk,
    coalesce(j.unstall_risk,device_all_risk.unstall_risk,1000) as unstall_risk_p,
    coalesce(j.unstall_risk,device_all_risk.unstall_risk,0) as unstall_risk_n,
    nvl(k.install_risk,device_all_risk.install_risk) as install_risk,
    coalesce(k.install_risk,device_all_risk.install_risk,1000) as install_risk_p,
    coalesce(k.install_risk,device_all_risk.install_risk,0) as install_risk_n,
    nvl(l.strange_app_risk,device_all_risk.strange_app_risk) as strange_app_risk,
    coalesce(l.strange_app_risk,device_all_risk.strange_app_risk,0) as strange_app_risk_n,
    nvl(m.bssidcnt_risk,device_all_risk.bssidcnt_risk) as bssidcnt_risk,
    coalesce(m.bssidcnt_risk,device_all_risk.bssidcnt_risk,1000) as bssidcnt_risk_p,
    coalesce(m.bssidcnt_risk,device_all_risk.bssidcnt_risk,0) as bssidcnt_risk_n,
    nvl(n.network_risk,device_all_risk.network_risk) as network_risk,
    coalesce(n.network_risk,device_all_risk.network_risk,1000) as network_risk_p,
    coalesce(n.network_risk,device_all_risk.network_risk,0) as network_risk_n,
    nvl(o.distance_risk,device_all_risk.distance_risk) as distance_risk,
    coalesce(o.distance_risk,device_all_risk.distance_risk,1000) as distance_risk_p,
    coalesce(o.distance_risk,device_all_risk.distance_risk,0) as distance_risk_n,
    nvl(p.gps_ip_vs_risk,device_all_risk.gps_ip_vs_risk) as gps_ip_vs_risk,
    coalesce(p.gps_ip_vs_risk,device_all_risk.gps_ip_vs_risk,1000) as gps_ip_vs_risk_p,
    coalesce(p.gps_ip_vs_risk,device_all_risk.gps_ip_vs_risk,0) as gps_ip_vs_risk_n,
    nvl(q.ip_risk,device_all_risk.ip_risk) as ip_risk,
    coalesce(q.ip_risk,device_all_risk.ip_risk,1000) as ip_risk_p,
    coalesce(q.ip_risk,device_all_risk.ip_risk,0) as ip_risk_n,
    nvl(r.daili_ip_risk,device_all_risk.daili_ip_risk) as daili_ip_risk,
    coalesce(r.daili_ip_risk,device_all_risk.daili_ip_risk,0) as daili_ip_risk_n,
    nvl(s.unstall_freq_risk,device_all_risk.unstall_freq_risk) as unstall_freq_risk,
    coalesce(s.unstall_freq_risk,device_all_risk.unstall_freq_risk,0) as unstall_freq_risk_n,
    nvl(t.install_freq_risk,device_all_risk.install_freq_risk) as install_freq_risk,
    coalesce(t.install_freq_risk,device_all_risk.install_freq_risk,0) as install_freq_risk_n,
    nvl(u.ip_shang_risk,device_all_risk.ip_shang_risk) as ip_shang_risk,
    coalesce(u.ip_shang_risk,device_all_risk.ip_shang_risk,0) as ip_shang_risk_n,
    nvl(v.daili_ip_p_risk,device_all_risk.daili_ip_p_risk) as daili_ip_p_risk,
    coalesce(v.daili_ip_p_risk,device_all_risk.daili_ip_p_risk,0) as daili_ip_p_risk_n,
  case
    when device_all_risk.device is not null and device_all_risk.ifhaverisk <>1 then device_all_risk.ifhaverisk
    when e.device is null and f.device is null and g.device is null and h.device is null and i.device is null and j.device is null and k.device is null and l.device is null and m.device is null and n.device is null and o.device is null and p.device is null and q.device is null and r.device is null and s.device is null and t.device is null and u.device is null and v.device is null  then 1
  else 0
  end as ifhaverisk
    from
    (
      select device
      from $dim_id_mapping_android_sec_df_view
    ) as a
    left join
    (
      select device, risk as model_risk
      from $device_info_risk
    ) as e
    on a.device = e.device
    left join
    (
      select device, active_risk
      from $device_active_risk
    ) as f
    on a.device = f.device
    left join
    (
      select device, share_risk
      from $device_sharecount_risk
    ) as g
    on a.device = g.device
    left join
    (
      select device, oauth_risk
      from $device_oauthcount_risk
    ) as h
    on a.device = h.device
    left join
    (
      select device, app_risk
      from $device_applist_risk
    ) as i
    on a.device = i.device
    left join
    (
      select device, unstall_risk
      from $device_unstall_risk
    ) as j
    on a.device = j.device
    left join
    (
      select device, install_risk
      from $device_install_risk
    ) as k
    on a.device = k.device
    left join
    (
      select device, 1 as strange_app_risk
      from $device_strange_app_type_install_3month
      group by device
    ) as l
    on a.device = l.device
    left join
    (
      select device, bssidcnt_risk
      from $device_bssidcnt_risk
    ) as m
    on a.device = m.device
    left join
    (
      select device, network_risk
      from $device_network_risk
    ) as n
    on a.device = n.device
    left join
    (
      select device, distance_risk
      from $device_distance_risk
    ) as o
    on a.device = o.device
    left join
    (
      select device, gps_ip_vs_risk
      from $device_gps_ip_risk
    ) as p
    on a.device = p.device
    left join
    (
      select device, ip_risk
      from $device_ip_risk
    ) as q
    on a.device = q.device
    left join
    (
      select device, 1 as daili_ip_risk
      from $device_ip_proxy_3month
      group by device
    ) as r
    on a.device = r.device
    left join
    (
      select device, unstall_freq_risk
      from $device_unstall_freq_risk
    ) as s
    on a.device = s.device
    left join
    (
      select device, install_freq_risk
      from $device_install_freq_risk
    ) as t
    on a.device = t.device
    left join
    (
      select device, ip_shang_risk
      from $device_ip_entropy_risk
    ) as u
    on a.device = u.device
    left join
    (
      select device, daili_ip_p_risk
      from $device_ip_proxy_p_risk
    ) as v
    on a.device = v.device
    left join
    $device_all_risk_pre device_all_risk
    on a.device= device_all_risk.device
  ) as w
  group by device, imei_risk,imei_risk_p,imei_risk_n, mac_risk,mac_risk_p,mac_risk_n, serialno_risk,serialno_risk_p,serialno_risk_n, model_risk,model_risk_p,model_risk_n, active_risk,active_risk_p,active_risk_n, share_risk,share_risk_p, share_risk_n,oauth_risk,oauth_risk_p,oauth_risk_n, app_risk,app_risk_p,app_risk_n, unstall_risk,unstall_risk_p,unstall_risk_n, install_risk,install_risk_p,install_risk_n,strange_app_risk,strange_app_risk_n, bssidcnt_risk,bssidcnt_risk_p,bssidcnt_risk_n, network_risk,network_risk_p,network_risk_n, distance_risk,distance_risk_p,distance_risk_n, gps_ip_vs_risk,gps_ip_vs_risk_p,gps_ip_vs_risk_n, ip_risk,ip_risk_p,ip_risk_n, daili_ip_risk,daili_ip_risk_n, unstall_freq_risk,unstall_freq_risk_n, install_freq_risk,install_freq_risk_n, ip_shang_risk,ip_shang_risk_n, daili_ip_p_risk,daili_ip_p_risk_n,ifhaverisk
) t1
"


max_risk_fangda_scale=`hive -e"select max(risk_fangda_scale) from $device_all_risk_pre"`

max_risk=`hive -e"
select  max(risk)
from
(
  select device, imei_risk, mac_risk, serialno_risk, model_risk, active_risk, share_risk, oauth_risk, app_risk, unstall_risk, install_risk, strange_app_risk, bssidcnt_risk, network_risk, distance_risk, gps_ip_vs_risk, ip_risk, daili_ip_risk, unstall_freq_risk, install_freq_risk, ip_shang_risk, daili_ip_p_risk, (imei_risk + mac_risk + serialno_risk + 1.5*model_risk + 1.5*active_risk + share_risk + oauth_risk + 1.5*app_risk + unstall_risk + install_risk + 2*strange_app_risk + bssidcnt_risk + 0.5*network_risk + 0.3*distance_risk + gps_ip_vs_risk + ip_risk + 0.3*daili_ip_risk + unstall_freq_risk + install_freq_risk + ip_shang_risk + 2*daili_ip_p_risk) as risk, ifhaverisk, null_cnt
  from
  (
    select device,
           coalesce(imei_risk, risk_fangda_scale/$max_risk_fangda_scale) as imei_risk,
           coalesce(mac_risk, risk_fangda_scale/$max_risk_fangda_scale) as mac_risk,
           coalesce(serialno_risk, risk_fangda_scale/$max_risk_fangda_scale) as serialno_risk,
           coalesce(model_risk, risk_fangda_scale/$max_risk_fangda_scale) as model_risk,
           coalesce(active_risk, risk_fangda_scale/$max_risk_fangda_scale) as active_risk,
           coalesce(share_risk, risk_fangda_scale/$max_risk_fangda_scale) as share_risk,
           coalesce(oauth_risk, risk_fangda_scale/$max_risk_fangda_scale) as oauth_risk,
           coalesce(app_risk, risk_fangda_scale/$max_risk_fangda_scale) as app_risk,
           coalesce(unstall_risk, risk_fangda_scale/$max_risk_fangda_scale) as unstall_risk,
           coalesce(install_risk, risk_fangda_scale/$max_risk_fangda_scale) as install_risk,
           coalesce(strange_app_risk, 0) as strange_app_risk,
           coalesce(bssidcnt_risk, risk_fangda_scale/$max_risk_fangda_scale) as bssidcnt_risk,
           coalesce(network_risk, risk_fangda_scale/$max_risk_fangda_scale) as network_risk,
           coalesce(distance_risk, risk_fangda_scale/$max_risk_fangda_scale) as distance_risk,
           coalesce(gps_ip_vs_risk, risk_fangda_scale/$max_risk_fangda_scale) as gps_ip_vs_risk,
           coalesce(ip_risk, risk_fangda_scale/$max_risk_fangda_scale) as ip_risk,
           coalesce(daili_ip_risk, 0) as daili_ip_risk,
           coalesce(unstall_freq_risk, risk_fangda_scale/$max_risk_fangda_scale) as unstall_freq_risk,
           coalesce(install_freq_risk, risk_fangda_scale/$max_risk_fangda_scale) as install_freq_risk,
           coalesce(ip_shang_risk, risk_fangda_scale/$max_risk_fangda_scale) as ip_shang_risk,
           coalesce(daili_ip_p_risk, 0) as daili_ip_p_risk,
           ifhaverisk,null_cnt
  from $device_all_risk_pre
  )t
)t2
"`
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

insert overwrite table $device_all_risk
select  device, imei_risk, mac_risk, serialno_risk, model_risk, active_risk, share_risk, oauth_risk, app_risk, unstall_risk, install_risk, strange_app_risk, bssidcnt_risk, network_risk, distance_risk, gps_ip_vs_risk, ip_risk, daili_ip_risk, unstall_freq_risk, install_freq_risk, ip_shang_risk, daili_ip_p_risk, risk as risk_all, 100*risk/${max_risk} as risk_scale, null_cnt, ifhaverisk
from
(
  select device, imei_risk, mac_risk, serialno_risk, model_risk, active_risk, share_risk, oauth_risk, app_risk, unstall_risk, install_risk, strange_app_risk, bssidcnt_risk, network_risk, distance_risk, gps_ip_vs_risk, ip_risk, daili_ip_risk, unstall_freq_risk, install_freq_risk, ip_shang_risk, daili_ip_p_risk, (imei_risk + mac_risk + serialno_risk + 1.5*model_risk + 1.5*active_risk + share_risk + oauth_risk + 1.5*app_risk + unstall_risk + install_risk + 2*strange_app_risk + bssidcnt_risk + 0.5*network_risk + 0.3*distance_risk + gps_ip_vs_risk + ip_risk + 0.3*daili_ip_risk + unstall_freq_risk + install_freq_risk + ip_shang_risk + 2*daili_ip_p_risk) as risk, ifhaverisk, null_cnt
  from
  (
    select device,
           coalesce(imei_risk, risk_fangda_scale/$max_risk_fangda_scale) as imei_risk,
           coalesce(mac_risk, risk_fangda_scale/$max_risk_fangda_scale) as mac_risk,
           coalesce(serialno_risk, risk_fangda_scale/$max_risk_fangda_scale) as serialno_risk,
           coalesce(model_risk, risk_fangda_scale/$max_risk_fangda_scale) as model_risk,
           coalesce(active_risk, risk_fangda_scale/$max_risk_fangda_scale) as active_risk,
           coalesce(share_risk, risk_fangda_scale/$max_risk_fangda_scale) as share_risk,
           coalesce(oauth_risk, risk_fangda_scale/$max_risk_fangda_scale) as oauth_risk,
           coalesce(app_risk, risk_fangda_scale/$max_risk_fangda_scale) as app_risk,
           coalesce(unstall_risk, risk_fangda_scale/$max_risk_fangda_scale) as unstall_risk,
           coalesce(install_risk, risk_fangda_scale/$max_risk_fangda_scale) as install_risk,
           coalesce(strange_app_risk, 0) as strange_app_risk,
           coalesce(bssidcnt_risk, risk_fangda_scale/$max_risk_fangda_scale) as bssidcnt_risk,
           coalesce(network_risk, risk_fangda_scale/$max_risk_fangda_scale) as network_risk,
           coalesce(distance_risk, risk_fangda_scale/$max_risk_fangda_scale) as distance_risk,
           coalesce(gps_ip_vs_risk, risk_fangda_scale/$max_risk_fangda_scale) as gps_ip_vs_risk,
           coalesce(ip_risk, risk_fangda_scale/$max_risk_fangda_scale) as ip_risk,
           coalesce(daili_ip_risk, 0) as daili_ip_risk,
           coalesce(unstall_freq_risk, risk_fangda_scale/$max_risk_fangda_scale) as unstall_freq_risk,
           coalesce(install_freq_risk, risk_fangda_scale/$max_risk_fangda_scale) as install_freq_risk,
           coalesce(ip_shang_risk, risk_fangda_scale/$max_risk_fangda_scale) as ip_shang_risk,
           coalesce(daili_ip_p_risk, 0) as daili_ip_p_risk,
           ifhaverisk,null_cnt
  from $device_all_risk_pre
  )t
)t2
group by device, imei_risk, mac_risk, serialno_risk, model_risk, active_risk, share_risk, oauth_risk, app_risk, unstall_risk, install_risk, strange_app_risk, bssidcnt_risk, network_risk, distance_risk, gps_ip_vs_risk, ip_risk, daili_ip_risk, unstall_freq_risk, install_freq_risk, ip_shang_risk, daili_ip_p_risk, risk, null_cnt, ifhaverisk
"