#!/bin/bash
set -x -e

: '
@owner:liuyanqiang
@describe:device用户标签画像full表取每日增量，匹配上app名字，输出给ga使用
@risk:使用该表风险——工作地、居住地、常驻地标签是月更新的，更新时不会刷新processtime_all值，会造成大量设备更新工作地、居住地、常驻地时，不会出现在表中
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh


##input
#rp_device_profile_full_view=dm_mobdi_report.rp_device_profile_full_view

##mapping
#dim_apppkg_info=dim_sdk_mapping.dim_apppkg_info

##output
#rp_device_profile_label_appname_di=dm_mobdi_report.rp_device_profile_label_appname_di

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.map.memory.mb=10240;
set mapreduce.map.java.opts='-Xmx10g';
set mapreduce.child.map.java.opts='-Xmx10g';
set mapreduce.reduce.memory.mb=10240;
set mapreduce.reduce.java.opts='-Xmx10240M';
set mapreduce.child.reduce.java.opts='-Xmx10240M';
insert overwrite table $rp_device_profile_label_appname_di PARTITION(day='$day')
select t1.device,carrier,network,cell_factory,sysver,model,model_level,screensize,country,province,city,city_level,
       country_cn,province_cn,city_cn,breaked,city_level_1001,public_date,identity,gender,agebin,car,married,edu,income,
       house,kids,occupation,industry,life_stage,special_time,consum_level,agebin_1001,tag_list,repayment,segment,applist,
       tot_install_apps,nationality,nationality_cn,last_active,group_list,first_active_time,catelist,permanent_country,
       permanent_province,permanent_city,permanent_country_cn,permanent_province_cn,permanent_city_cn,workplace,residence,
       mapping_flag,model_flag,stats_flag,processtime,processtime_all,price,permanent_city_level,income_1001,
       t2.app_name_list
from
(
  select device,carrier,network,cell_factory,sysver,model,model_level,screensize,country,province,city,city_level,
         country_cn,province_cn,city_cn,breaked,city_level_1001,public_date,identity,gender,agebin,car,married,edu,income,
         house,kids,occupation,industry,life_stage,special_time,consum_level,agebin_1001,tag_list,repayment,segment,applist,
         tot_install_apps,nationality,nationality_cn,last_active,group_list,first_active_time,catelist,permanent_country,
         permanent_province,permanent_city,permanent_country_cn,permanent_province_cn,permanent_city_cn,workplace,residence,
         mapping_flag,model_flag,stats_flag,processtime,processtime_all,price,permanent_city_level,income_1001
  from $rp_device_profile_full_view
  where processtime_all='$day'
) t1
left join
(
  select a1.device,
         collect_list(
           if(a1.apppkg='unknown',null,
             named_struct('apppkg',a1.apppkg,'app_name',coalesce(a2.name,'')))
         ) as app_name_list
  from
  (
    select device,apppkg
    from $rp_device_profile_full_view
    lateral view explode(split(applist,',')) app as apppkg
    where processtime_all='$day'
  ) a1
  left join
  $dim_apppkg_info a2 on a1.apppkg = a2.apppkg
  group by a1.device
) t2 on t1.device=t2.device
"
