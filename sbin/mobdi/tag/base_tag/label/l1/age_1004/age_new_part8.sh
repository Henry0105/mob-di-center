#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_year=$(date -d "${day} -1 year" "+%Y%m%d")
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh
insertday=${day}_muid
#dim_age_app_category_final_new=dim_mobdi_mapping.dim_age_app_category_final_new
#category_mapping_table=dm_mobdi_mapping.age_app_category_final_new

#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
#device_active_applist=dm_mobdi_master.device_active_applist

age_new_active_days_12="${dm_mobdi_tmp}.age_new_active_days_12"


hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
set mapreduce.map.java.opts=-Xmx10000m;
set mapreduce.map.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx10000m;
set mapreduce.reduce.memory.mb=9000;
set mapred.reduce.tasks=50000;
insert overwrite table $age_new_active_days_12 partition (day='$insertday')
select device
,max(case when cate_id='tgi2_18_5' then cnt_1 else 0 end ) as tgi2_18_5_act_day_12m
,max(case when cate_id='tgi2_18_6' then cnt_1 else 0 end ) as tgi2_18_6_act_day_12m
,max(case when cate_id='tgi2_45_54_1' then cnt_1 else 0 end ) as tgi2_45_54_1_act_day_12m
from
(
    select device,cate_id,count(day)/12 as cnt_1 
    from 
    (
        select device,cate_id,day
        from(
                select a1.*,COALESCE(a2.cate_id,'other') as cate_id
                from(
                    select * 
                    from $dws_device_active_applist_di
                    where day between $day_before_one_year and $day
                    
                )a1
                left join(
                    select *
                    from $dim_age_app_category_final_new

                )a2
                on a1.pkg = a2.pkg
            )t1
        group by device,cate_id,day
    )a
    group by device,cate_id
)t2
group by device;
"