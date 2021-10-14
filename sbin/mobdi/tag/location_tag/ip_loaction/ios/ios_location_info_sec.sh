#!/bin/sh
: '
@owner:zhoup
@describe:ios地理位置信息
@projectName:
@BusinessName:
@SourceTable:$dws_device_ip_info_di,$ios_id_mapping_sec_df_view,$ifid_ip_location_sec_df,$ifid_ip_location_sec_di
@TargetTable:$ifid_ip_location_sec_di,$ifid_ip_location_sec_df
@TableRelation:$dws_device_ip_info_di,$ios_id_mapping_sec_df_view->$ifid_ip_location_sec_di|$ifid_ip_location_sec_di->$ifid_ip_location_sec_df
'

set -x -e
day=$1


source /home/dba/mobdi_center/conf/hive-env.sh

##input:
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

##mapping:
#dim_id_mapping_ios_sec_df_view=dim_mobdi_mapping.dim_id_mapping_ios_sec_df_view
#ios_id_mapping_sec_df_view=dm_mobdi_mapping.ios_id_mapping_full_sec_view

##output:
#ifid_ip_location_sec_di=dm_mobdi_report.ifid_ip_location_sec_di
#ifid_ip_location_sec_df=dm_mobdi_report.ifid_ip_location_sec_df


:<<!
create table if not exists $ifid_ip_location_sec_di(
ifid            string comment 'ios唯一标识',
country         string comment '国家',
province        string comment '省份',
city            string comment '城市',
area            string comment '区域',
networktype     string comment '网络类型',
language        string comment'语言',
city_level      string comment'1000对应的城市等级',
city_level_1001      string comment'1001对应的城市等级'
)comment 'ios ip位置信息增量表'
partitioned by (day string comment'日期')
stored as orc;

create table if not exists $ifid_ip_location_sec_df(
ifid            string comment 'ios唯一标识',
country         string comment '国家',
province        string comment '省份',
city            string comment '城市',
area            string comment '区域',
networktype     string comment '网络类型',
language        string comment'语言',
city_level      string comment'1000对应的城市等级',
city_level_1001      string comment'1001对应的城市等级'
)comment 'ios ip位置信息全量表'
partitioned by (day string comment'日期')
stored as orc;
!


hive -e "
set hive.exec.parallel=true;
set mapred.job.name=ios_location_info_sec;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

insert overwrite table $ifid_ip_location_sec_di partition(day=$day)
select b.ifid,a.country,a.province,a.city,a.area,a.networktype,a.language,nvl(c.level,'-1') as city_level,nvl(d.level,'-1') as city_level_1001
from(
select device,country,province,city,area,networktype,language 
from(
  select device,language,network as networktype,country,province,city,area,
                  ROW_NUMBER() OVER(PARTITION BY device ORDER BY timestamp desc) as rank
    from $dws_device_ip_info_di
      where plat=2 and day=$day
  )tt where rank=1
)a
inner join
(select device,ifids as ifid
        from $dim_id_mapping_ios_sec_df_view lateral view explode(split(ifid,',')) t as ifids
        where ifids<>''        --删除了ifid过滤脏数据条件
)b
on a.device=b.device
left join
(select * from dm_sdk_mapping.mapping_city_level_par where version='1000') c
on a.city = c.city_code
left join
(select * from dm_sdk_mapping.mapping_city_level_par where version='1001') d
on a.city = d.city_code
"

hive -e "
set hive.exec.parallel=true;
set mapred.job.name=ios_location_info_sec;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

insert overwrite table $ifid_ip_location_sec_df
select ifid,country,province,city,processtime,area,networktype,language,nvl(city_level,'-1') as city_level,nvl(city_level_1001,'-1') as city_level_1001
from(
  select ifid,country,province,city,processtime,area,networktype,language,city_level,city_level_1001,ROW_NUMBER() OVER(PARTITION BY ifid ORDER BY processtime desc) as rank
    from(
      select ifid,country,province,city,area,networktype,language,city_level,city_level_1001,processtime
        from $ifid_ip_location_sec_df
      union all
      select ifid,country,province,city,area,networktype,language,city_level,city_level_1001,day as processtime
        from $ifid_ip_location_sec_di where day=$day
    )t1
)t2
where rank=1
"
