#!/bin/bash

set -e -x

: '
@owner:luost
@describe:superset中添加核心数据维度监控看榜-地理线
@projectName:mob_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
dwd_device_location_aggr_statiscs2_w=mob_dashboard.dwd_device_location_aggr_statiscs2_w
ads_bi_device_location_aggr_statiscs_w=mob_dashboard.ads_bi_device_location_aggr_statiscs_w

#mapping
#mapping_area_par=dm_sdk_mapping.mapping_area_par

#output
device_gps_dau_info_by_location_aggr=mob_dashboard.device_gps_dau_info_by_location_aggr

:<<!
create table mob_dashboard.device_gps_dau_info_by_location_aggr(
    cnt bigint comment '中国大陆GPS报点量',
    dau bigint comment '中国大陆GPS有效日活',
    frequency double comment '中国大陆单设备GPS日报点频次'
)
partitioned by (day string comment '日期')
stored as orc;
!

area_lastpar=`hive -S -e "show partitions dm_sdk_mapping.mapping_area_par" |tail -n 1 `

hive -v -e "
set mapreduce.job.queuename=root.yarn_mobdashboard.mobdashboard;

insert overwrite table $device_gps_dau_info_by_location_aggr partition(day = '$day')
select x.cnt as cnt,
       y.cnt as dau,
       x.cnt/y.cnt as frequency
from 
(
    select sum(anals_value) as cnt
    from
    (
        select city,
               anals_value
        from $dwd_device_location_aggr_statiscs2_w
        where type = 'gps'
        and day = '$day'
    ) a
    left join 
    (
        select province_code,
               city_code,    
               province,
               city
        from $mapping_area_par
        where $area_lastpar
        and country = '中国'
        group by province_code,city_code,province,city
    ) b
    on a.city = b.city_code
    where b.province != '' and b.city != ''
)x
left join 
(
    select anals_value as cnt
    from $ads_bi_device_location_aggr_statiscs_w 
    where label = '4'  
    and anals_dim2 = 'cn'
    and day = '$day'
    and anals_dim1 = 'gps'
)y
on 1 = 1;
"