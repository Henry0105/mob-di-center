#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh

#dim_age_app_category_final_new=dim_mobdi_mapping.dim_age_app_category_final_new
#category_mapping_table=dm_mobdi_mapping.age_app_category_final_new
#label_device_pkg_install_uninstall_year_info_mf="rp_mobdi_app.label_device_pkg_install_uninstall_year_info_mf"

age_new_ratio_features_3m="$dm_mobdi_tmp.age_new_ratio_features_3m"


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
insert overwrite table $age_new_ratio_features_3m partition(day=$day) 
select
    device
,tgi1_18_24_6_3m_cnt
,round(COALESCE(tgi1_18_0_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_18_0_3m_ratio
from(
      select 
              device
            ,count( pkg ) as total_3m_cnt
           ,count(case when cate_id='tgi1_18_0' then 1 else null end) as tgi1_18_0_3m_cnt
           ,count(case when cate_id='tgi1_18_24_6' then 1 else null end) as tgi1_18_24_6_3m_cnt
      from(
             select
                b.device
                ,from_unixtime(unix_timestamp(b.day,'yyyyMMdd'),'yyyy-MM-dd') as date
                ,a.cate_id
                ,a.pkg 
                ,b.refine_final_flag 
                ,from_unixtime(unix_timestamp(b.first_day,'yyyyMMdd'),'yyyy-MM-dd') as first_day
                ,from_unixtime(unix_timestamp(b.flag_day,'yyyyMMdd'),'yyyy-MM-dd') as flag_day
                ,from_unixtime(unix_timestamp(b.update_day,'yyyyMMdd'),'yyyy-MM-dd') as update_day
                , day
            from
            (
                select pkg, cate_id
                from $dim_age_app_category_final_new
                group by pkg, cate_id
            )a 
            join
            (
                select device,pkg,refine_final_flag,first_day,flag_day,update_day,day
                from $label_device_pkg_install_uninstall_year_info_mf
                where day=$day and update_day between $day_before_one_month and $day
            )b
            on a.pkg=b.pkg
        )t1
         where datediff(date,update_day)<=90 and  datediff(date,flag_day)<=90
        group by device
    )t2 ;
"

hive -e "
alter table $age_new_ratio_features_3m drop partition(day< $day_before_one_month);
"