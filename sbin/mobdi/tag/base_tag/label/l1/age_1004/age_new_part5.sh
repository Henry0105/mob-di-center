#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh
insertday=${day}_muid
#dim_age_app_category_final_new=dim_mobdi_mapping.dim_age_app_category_final_new
#category_mapping_table=dm_mobdi_mapping.age_app_category_final_new

#label_device_pkg_install_uninstall_year_info_mf="rp_mobdi_app.label_device_pkg_install_uninstall_year_info_mf"

age_new_Ins_recency_features="${dm_mobdi_tmp}.age_new_Ins_recency_features"

hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
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
insert overwrite table  $age_new_Ins_recency_features partition (day='$insertday')
select
     device
    ,datediff(date_dt, cate7006_001_install_first_min) as cate7006_001_install_first_min_diff
    ,datediff(date_dt, tgi1_18_24_5_install_first_min) as tgi1_18_24_5_install_first_min_diff
    ,datediff(date_dt, tgi1_25_34_0_install_first_min) as tgi1_25_34_0_install_first_min_diff
    ,datediff(date_dt, tgi1_35_44_1_install_first_min) as tgi1_35_44_1_install_first_min_diff
    ,datediff(date_dt, tgi1_35_44_5_install_first_min) as tgi1_35_44_5_install_first_min_diff
    ,datediff(date_dt, tgi1_45_54_1_install_first_min) as tgi1_45_54_1_install_first_min_diff
    ,datediff(date_dt, tgi1_55_0_install_first_min) as tgi1_55_0_install_first_min_diff
    ,datediff(date_dt, tgi2_45_54_5_install_first_min) as tgi2_45_54_5_install_first_min_diff


    ,datediff(date_dt, cate7001_002_install_first_max) as cate7001_002_install_first_max_diff
    ,datediff(date_dt, cate7008_002_install_first_max) as cate7008_002_install_first_max_diff
    ,datediff(date_dt, tgi1_18_0_install_first_max) as tgi1_18_0_install_first_max_diff
    ,datediff(date_dt, tgi1_18_1_install_first_max) as tgi1_18_1_install_first_max_diff
    ,datediff(date_dt, tgi1_45_54_0_install_first_max) as tgi1_45_54_0_install_first_max_diff
    ,datediff(date_dt, tgi1_55_6_install_first_max) as tgi1_55_6_install_first_max_diff
    ,datediff(date_dt, tgi2_25_34_5_install_first_max) as tgi2_25_34_5_install_first_max_diff
    ,datediff(date_dt, tgi2_35_44_5_install_first_max) as tgi2_35_44_5_install_first_max_diff
    ,datediff(date_dt, tgi2_55_1_install_first_max) as tgi2_55_1_install_first_max_diff
from
    (select
        device
        ,min(case when cate_id='cate7006_001' then update_day else null end ) as cate7006_001_install_first_min
        ,min(case when cate_id='tgi1_18_24_5' then update_day else null end ) as tgi1_18_24_5_install_first_min
        ,min(case when cate_id='tgi1_25_34_0' then update_day else null end ) as tgi1_25_34_0_install_first_min
        ,min(case when cate_id='tgi1_35_44_1' then update_day else null end ) as tgi1_35_44_1_install_first_min
        ,min(case when cate_id='tgi1_35_44_5' then update_day else null end ) as tgi1_35_44_5_install_first_min
        ,min(case when cate_id='tgi1_45_54_1' then update_day else null end ) as tgi1_45_54_1_install_first_min
        ,min(case when cate_id='tgi1_55_0' then update_day else null end ) as tgi1_55_0_install_first_min
        ,min(case when cate_id='tgi2_45_54_5' then update_day else null end ) as tgi2_45_54_5_install_first_min



        ,max(case when cate_id='cate7001_002' then update_day else null end ) as cate7001_002_install_first_max
        ,max(case when cate_id='cate7008_002' then update_day else null end ) as cate7008_002_install_first_max
        ,max(case when cate_id='tgi1_18_0' then update_day else null end ) as tgi1_18_0_install_first_max
        ,max(case when cate_id='tgi1_18_1' then update_day else null end ) as tgi1_18_1_install_first_max
        ,max(case when cate_id='tgi1_45_54_0' then update_day else null end ) as tgi1_45_54_0_install_first_max
        ,max(case when cate_id='tgi1_55_6' then update_day else null end ) as tgi1_55_6_install_first_max
        ,max(case when cate_id='tgi2_25_34_5' then update_day else null end ) as tgi2_25_34_5_install_first_max
        ,max(case when cate_id='tgi2_35_44_5' then update_day else null end ) as tgi2_35_44_5_install_first_max
        ,max(case when cate_id='tgi2_55_1' then update_day else null end ) as tgi2_55_1_install_first_max

        ,date_dt 
    from(
            select 
                device,cate_id,pkg,refine_final_flag
                ,from_unixtime(unix_timestamp(update_day,'yyyyMMdd'),'yyyy-MM-dd') as update_day
                ,from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd') as date_dt
            from
                (select
                    b.device,
                    a.cate_id, 
                    a.pkg ,
                    b.refine_final_flag ,
                    b.update_day ,
                    b.day 
                from
                    (
                        select pkg, cate_id
                        from $dim_age_app_category_final_new
                        group by pkg, cate_id
                    )a 
                    join
                    (
                        select device,pkg,refine_final_flag,update_day,flag_day,day
                        from $label_device_pkg_install_uninstall_year_info_mf
                        where day=$day and update_day between $day_before_one_month and $day
                        and refine_final_flag in (0,1)
                        group by device,pkg,refine_final_flag,update_day,flag_day,day
                    )b
                    on a.pkg=b.pkg
              )t1
        )t2
    where datediff(date_dt,update_day)<=365
    group by device,date_dt
)t3;
"

#hive -e "
#alter table $age_new_Ins_recency_features drop partition(day< $day_before_one_month);
#"