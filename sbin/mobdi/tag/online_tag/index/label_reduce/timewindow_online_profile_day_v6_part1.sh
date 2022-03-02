#!/usr/bin/env bash
set -x -e

day=$1
pre_day=`date -d "$day -1 months" +%Y%m%d`
version="${day}_monthly_bak"

# input
ads_ppx_score_weekly=dm_mobdi_report.ads_ppx_score_weekly
rp_device_outing=dm_mobdi_report.rp_device_outing
ads_device_frequencry_position_classify=dm_mobdi_report.ads_device_frequencry_position_classify
travel_label_monthly=dm_mobdi_report.travel_label_monthly
ad_device_whitelist_full_sec_new=rp_finance_anticheat.ad_device_whitelist_full_sec_new
ads_device_medical_location_cnt_mi=dm_mobdi_report.ads_device_medical_location_cnt_mi
device_language=dm_mobdi_report.device_language
label_l1_catelist_di=dm_mobdi_report.label_l1_catelist_di
label_l1_taglist_di=dm_mobdi_report.label_l1_taglist_di
device_ip_info=dm_mobdi_topic.dws_device_ip_info_di
label_l1_applist_refine_cnt_di=dm_mobdi_report.label_l1_applist_refine_cnt_di
dwd_device_info_di=dm_mobdi_master.dwd_device_info_di

# output
timewindow_online_profile_day_v6_part1=dm_mobdi_report.timewindow_online_profile_day_v6_part1


HADOOP_USER_NAME=dba hive -e "
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=500;
set mapred.map.tasks.speculative.execution=false;
set mapred.reduce.tasks.speculative.execution=false;
set hive.mapred.reduce.tasks.speculative.execution=false;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION map_to_str AS 'com.youzu.mob.java.map.MapToString';
create temporary function map_concat as 'com.youzu.mob.java.map.MapConcat';
create temporary function map_agg as 'com.youzu.mob.java.map.MapAgg';


insert overwrite table $timewindow_online_profile_day_v6_part1 partition(day='${day}')
select trim(lower(device)) as device,map_agg(profile) as  profile from
(
  select device,map(
    '5729_1000' ,coalesce(cast(cast(score1 as decimal(38,37)) as string),''),
    '5730_1000' ,coalesce(cast(cast(score2 as decimal(38,37)) as string),''),
    '5731_1000' ,coalesce(cast(cast(probability  as decimal(38,37)) as string),''),
    '5732_1000' ,coalesce(cast(score as string),''),
    '5733_1000' ,risk_rank
    ) as profile
  from $ads_ppx_score_weekly where day='$day'

  union all

  select device,map(
    '5341_1000',tot_times,
    '5342_1000',per_country
  ) as profile
  from $rp_device_outing where day='$day'

  union all

  select device,map(
    '5356_1000',cast(type as string)
  ) as profile
  from $ads_device_frequencry_position_classify where day='$pre_day'

  union all

  select device,map(
    '5331_1000',travel_area,
    '5332_1000',country,
    '5333_1000',travel_type,
    '5334_1000',vaca_flag,
    '5335_1000',cast(province_flag as string),
    '5336_1000',province,
    '5337_1000',city,
    '5338_1000',travel_time,
    '5339_1000',traffic,
    '5340_1000',travel_channel
  ) as profile
  from $travel_label_monthly where day='$day'

  union all

  select device,map(
    '5359_1000',coalesce(cast(dev_info_abnormality as string),''),
    '5360_1000',coalesce(cast(risk_result as string),''),
    '5361_1000',coalesce(cast(risk_score as string),'')
  ) as profile
  from $ad_device_whitelist_full_sec_new where dt='$day'

  union all

  select device,map(
    '5357_1000',cast(cnt_hospital as string)
  ) as profile
  from $ads_device_medical_location_cnt_mi where day='$day'

  union all

  select device,map(
    '15_1000',language_code
  ) as profile
  from (select * from $device_language where day='$day' )a

  union all

  select device,map('5920_1000',catelist) as profile from $label_l1_catelist_di where day='$day'

  union all

  select device,map('5919_1000',tag_list) as profile
  from $label_l1_taglist_di
  where day='$day'

  union all
  select device,map(
    '5275_1000',last_active
  ) as profile
  from  (select device,day as last_active from $device_ip_info  where day='$day' and plat='1'  group by device,day) c

  union all

  select device,map(
    '4464_1000',cast(cnt as string)
  ) as profile
  from $label_l1_applist_refine_cnt_di where day='$day'

  union all

  select device,map(
    '5325_1000',price,
    '24_1000',breaked,
    '23_1000',screensize,
    '22_1000',sysver,
    '19_1000',model_level
  ) as profile
  from  (
     select device,
        case when price='' or price is null then 'unknown' else price end as price,
        case when breaked_clean is null then 'unknown' else lower(cast(breaked_clean as string)) end as breaked,
        case when screensize_clean='' then 'unknown' else screensize_clean end as screensize,
        case when sysver_clean='' then 'unknown' else sysver_clean end as sysver,
        case when price is null or price = '' then -1
                when price > 2500 then 0
                when price >= 1000 and price <= 2500 then 1
                when price > 0 and price < 1000 then 2
                else -1 end as model_level
     from $dwd_device_info_di
     where day='$day'  and plat = '1'
   ) a
)union_source
where trim(lower(device)) rlike '^[a-f0-9]{40}$' and trim(device)!='0000000000000000000000000000000000000000'
group by trim(lower(device))
cluster by trim(lower(device))
;
"
