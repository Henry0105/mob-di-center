#!/bin/sh

day=$1

p1months=`date -d "$day -1 months" +%Y%m%d`
p2months=`date -d "$day -2 months" +%Y%m%d`
p3months=`date -d "$day -3 months" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh
#input
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di
#out
device_network_risk=${dm_mobdi_tmp}.device_network_risk

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with netwrok_cnt_and_ifwifi_1month as(
  select device, count(*) as network_cnt, sum(wifi_help) as ifwifi
  from 
  (
    select device, network, 
    case 
      when network = 'wifi' then 1 
    else 0 
    end as wifi_help
    from 
    (
      select device, network
      from 
      (
        select device, trim(t.network_split) as network
        from $dws_device_ip_info_di
        lateral view explode(split(network, ',')) t as network_split
        where length(trim(t.network_split)) > 0
        and  day >= '$p1months' and day <= '$day' and length(trim(network)) > 0
      ) as network_explode 
      group by device, network
    ) as b 
  ) as c 
  group by device
),
netwrok_cnt_and_ifwifi_2month as(
  select device, count(*) as network_cnt, sum(wifi_help) as ifwifi
  from 
  (
    select device, network, 
    case 
      when network = 'wifi' then 1 
    else 0 
    end as wifi_help
    from 
    (
      select device, network
      from 
      (
        select device, trim(t.network_split) as network
        from $dws_device_ip_info_di
        lateral view explode(split(network, ',')) t as network_split
        where length(trim(t.network_split)) > 0
        and day >= '$p2months' and day <= '$day' and length(trim(network)) > 0
      ) as network_explode 
      group by device, network
    ) as b 
  ) as c 
  group by device
),
netwrok_cnt_and_ifwifi_3month as(
  select device, count(*) as network_cnt, sum(wifi_help) as ifwifi
  from 
  (
    select device, network, 
    case 
      when network = 'wifi' then 1 
    else 0 
    end as wifi_help
    from 
    (
      select device, network
      from 
      (
        select device, trim(t.network_split) as network
        from $dws_device_ip_info_di
        lateral view explode(split(network, ',')) t as network_split
        where length(trim(t.network_split)) > 0
        and  day >= '$p3months' and day <= '$day' and length(trim(network)) > 0
      ) as network_explode 
      group by device, network
    ) as b 
  ) as c 
  group by device
),
network_risk_pre as(
  select device, flag, risk as network_risk
  from 
  (
    select device, flag, risk, row_number() over(partition by device order by flag desc) as num
    from 
    (
      select device, 1 as flag, 0.4 as risk
      from netwrok_cnt_and_ifwifi_1month
      where ifwifi = 1 and network_cnt = 1
      union all 
      select device, 2 as flag, 0.6 as risk
      from netwrok_cnt_and_ifwifi_2month
      where ifwifi = 1 and network_cnt = 1
      union all 
      select device, 3 as flag, 0.8 as risk
      from netwrok_cnt_and_ifwifi_3month
      where ifwifi = 1 and network_cnt = 1
    ) as a 
  ) as b 
  where num = 1
)
insert overwrite  table $device_network_risk
select netwrok_cnt_and_ifwifi_3month.device, 
case 
  when network_risk_pre.network_risk is null then 0 
  when network_risk_pre.network_risk is not null then network_risk_pre.network_risk
end as network_risk
from netwrok_cnt_and_ifwifi_3month
left join network_risk_pre 
on netwrok_cnt_and_ifwifi_3month.device = network_risk_pre.device
"