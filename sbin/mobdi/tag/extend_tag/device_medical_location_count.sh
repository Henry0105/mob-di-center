#!/bin/sh


set -e -x

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#input
#dm_mobdi_topic.dws_device_lbs_poi_10type_di
#rp_mobdi_app.rp_device_profile_full_view
#out
#dm_mobdi_report.ads_device_medical_location_cnt_mi

day=$1

p3month=`date -d "$day -3 months" +%Y%m%d`

hive -e"
insert overwrite  table $ads_device_medical_location_cnt_mi partition(day=$day)
select rs.device 
        ,case when profile_full.identity=112 or profile_full.occupation=15 then 0 else rs.day_count end as cnt_hospital
from 
(
    select device
            ,count(1) as day_count
    from
    (
        select device,day
        from $dws_device_lbs_poi_10type_di
        where day<='$day' and day>='$p3month' 
                and type_9 is not null 
                and type_9.name is not null and type_9.name!='' and type_9.name not like '%宠物%'
        group by device,day
    ) lbs_poi
    group by device    
) rs
left join $rp_device_profile_full_view profile_full
on rs.device=profile_full.device
"
