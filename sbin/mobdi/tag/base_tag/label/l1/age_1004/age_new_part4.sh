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

age_new_newIns_recency_features="${dm_mobdi_tmp}.age_new_newIns_recency_features"



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
insert overwrite table  $age_new_newIns_recency_features partition (day=$insertday)
select
     device
    ,datediff(date_dt, cate7011_998_newIns_first_min) as cate7011_998_newIns_first_min_diff
    ,datediff(date_dt, tgi1_18_24_0_newIns_first_min) as tgi1_18_24_0_newIns_first_min_diff
    ,datediff(date_dt, tgi1_25_34_0_newIns_first_min) as tgi1_25_34_0_newIns_first_min_diff
    ,datediff(date_dt, tgi1_35_44_1_newIns_first_min) as tgi1_35_44_1_newIns_first_min_diff
    ,datediff(date_dt, tgi1_55_6_newIns_first_min) as tgi1_55_6_newIns_first_min_diff
    ,datediff(date_dt, tgi2_18_0_newIns_first_min) as tgi2_18_0_newIns_first_min_diff
    ,datediff(date_dt, tgi2_55_1_newIns_first_min) as tgi2_55_1_newIns_first_min_diff
    ,datediff(date_dt, cate7016_002_newIns_first_max) as cate7016_002_newIns_first_max_diff
    ,datediff(date_dt, tgi2_18_24_4_newIns_first_max) as tgi2_18_24_4_newIns_first_max_diff
    ,datediff(date_dt, tgi2_18_24_5_newIns_first_max) as tgi2_18_24_5_newIns_first_max_diff
    ,datediff(date_dt, tgi2_25_34_0_newIns_first_max) as tgi2_25_34_0_newIns_first_max_diff
    ,datediff(date_dt, tgi2_25_34_1_newIns_first_max) as tgi2_25_34_1_newIns_first_max_diff
    ,datediff(date_dt, tgi2_25_34_6_newIns_first_max) as tgi2_25_34_6_newIns_first_max_diff
    ,datediff(date_dt, tgi2_55_0_newIns_first_max) as tgi2_55_0_newIns_first_max_diff
from
    (select
        device
        ,min(case when cate_id='cate7011_998' then first_day else null end ) as cate7011_998_newIns_first_min
        ,min(case when cate_id='tgi1_18_24_0' then first_day else null end ) as tgi1_18_24_0_newIns_first_min
        ,min(case when cate_id='tgi1_25_34_0' then first_day else null end ) as tgi1_25_34_0_newIns_first_min
        ,min(case when cate_id='tgi1_35_44_1' then first_day else null end ) as tgi1_35_44_1_newIns_first_min
        ,min(case when cate_id='tgi1_55_6' then first_day else null end ) as tgi1_55_6_newIns_first_min
        ,min(case when cate_id='tgi2_18_0' then first_day else null end ) as tgi2_18_0_newIns_first_min
        ,min(case when cate_id='tgi2_55_1' then first_day else null end ) as tgi2_55_1_newIns_first_min

        ,max(case when cate_id='cate7016_002' then first_day else null end ) as cate7016_002_newIns_first_max
        ,max(case when cate_id='tgi2_18_24_4' then first_day else null end ) as tgi2_18_24_4_newIns_first_max
        ,max(case when cate_id='tgi2_18_24_5' then first_day else null end ) as tgi2_18_24_5_newIns_first_max
        ,max(case when cate_id='tgi2_25_34_0' then first_day else null end ) as tgi2_25_34_0_newIns_first_max
        ,max(case when cate_id='tgi2_25_34_1' then first_day else null end ) as tgi2_25_34_1_newIns_first_max
        ,max(case when cate_id='tgi2_25_34_6' then first_day else null end ) as tgi2_25_34_6_newIns_first_max
        ,max(case when cate_id='tgi2_55_0' then first_day else null end ) as tgi2_55_0_newIns_first_max
        ,date_dt 
    from(
            select 
                device,cate_id,pkg,refine_final_flag
                ,from_unixtime(unix_timestamp(first_day,'yyyyMMdd'),'yyyy-MM-dd') as first_day
                ,from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd') as date_dt
            from
                (select
                    b.device,
                    a.cate_id, 
                    a.pkg ,
                    b.refine_final_flag ,
                    b.first_day ,
                    b.day 
                from
                    (
                        select pkg, cate_id
                        from $dim_age_app_category_final_new
                        where cate_id in ('cate7011_998','cate7016_002','tgi1_18_24_0','tgi1_25_34_0','tgi1_35_44_1','tgi1_55_6','tgi2_18_0','tgi2_18_24_4','tgi2_18_24_5','tgi2_25_34_0','tgi2_25_34_1','tgi2_25_34_6','tgi2_55_0','tgi2_55_1')
                        group by pkg, cate_id
                    )a 
                    join
                    (
                        select device,pkg,refine_final_flag,first_day,flag_day,update_day,day
                        from $label_device_pkg_install_uninstall_year_info_mf
                        where day=$day and update_day between $day_before_one_month and $day
                        and refine_final_flag in (0,1)
                    )b
                    on a.pkg=b.pkg
              )t1
        )t2
    where datediff(date_dt,first_day)<=365
    group by device,date_dt
)t3;
"
#hive -e "
#alter table $age_new_newIns_recency_features drop partition (day< $day_before_one_month);
#"