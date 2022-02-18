#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_year=$(date -d "${day} -1 year" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh
insertday=${day}_muid
#dim_age_app_category_final_new=dim_mobdi_mapping.dim_age_app_category_final_new
#category_mapping_table=dm_mobdi_mapping.age_app_category_final_new

#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
#device_active_applist=dm_mobdi_master.device_active_applist
age_new_active_recency_features="${dm_mobdi_tmp}.age_new_active_recency_features"


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
insert overwrite table $age_new_active_recency_features partition (day='$insertday')
select device
,datediff(date_dt,cate17_active_max_day) as cate17_active_max_day_diff
,datediff(date_dt,cate7001_002_active_max_day) as cate7001_002_active_max_day_diff
,datediff(date_dt,cate7005_001_active_max_day) as cate7005_001_active_max_day_diff
,datediff(date_dt,cate7010_001_active_max_day) as cate7010_001_active_max_day_diff
,datediff(date_dt,cate7011_998_active_max_day) as cate7011_998_active_max_day_diff
,datediff(date_dt,cate7013_008_active_max_day) as cate7013_008_active_max_day_diff
,datediff(date_dt,cate7015_001_active_max_day) as cate7015_001_active_max_day_diff
,datediff(date_dt,fin_46_active_max_day) as fin_46_active_max_day_diff
,datediff(date_dt,tgi1_18_0_active_max_day) as tgi1_18_0_active_max_day_diff
,datediff(date_dt,tgi1_35_44_1_active_max_day) as tgi1_35_44_1_active_max_day_diff
,datediff(date_dt,tgi1_35_44_5_active_max_day) as tgi1_35_44_5_active_max_day_diff
,datediff(date_dt,tgi1_45_54_5_active_max_day) as tgi1_45_54_5_active_max_day_diff
,datediff(date_dt,tgi2_18_0_active_max_day) as tgi2_18_0_active_max_day_diff
,datediff(date_dt,tgi2_18_24_5_active_max_day) as tgi2_18_24_5_active_max_day_diff
,datediff(date_dt,tgi2_25_34_5_active_max_day) as tgi2_25_34_5_active_max_day_diff
,datediff(date_dt,tgi2_35_44_5_active_max_day) as tgi2_35_44_5_active_max_day_diff
,datediff(date_dt,tgi2_35_44_6_active_max_day) as tgi2_35_44_6_active_max_day_diff
,datediff(date_dt,tgi2_45_54_1_active_max_day) as tgi2_45_54_1_active_max_day_diff
,datediff(date_dt,tgi2_55_0_active_max_day) as tgi2_55_0_active_max_day_diff
,datediff(date_dt,tgi2_55_1_active_max_day) as tgi2_55_1_active_max_day_diff
,datediff(date_dt,cate2_active_min_day) as cate2_active_min_day_diff
,datediff(date_dt,cate22_active_min_day) as cate22_active_min_day_diff
,datediff(date_dt,cate7011_993_active_min_day) as cate7011_993_active_min_day_diff
,datediff(date_dt,cate7012_012_active_min_day) as cate7012_012_active_min_day_diff
,datediff(date_dt,cate7019_107_active_min_day) as cate7019_107_active_min_day_diff
,datediff(date_dt,tgi1_18_24_0_active_min_day) as tgi1_18_24_0_active_min_day_diff
,datediff(date_dt,tgi1_18_6_active_min_day) as tgi1_18_6_active_min_day_diff
,datediff(date_dt,tgi1_25_34_0_active_min_day) as tgi1_25_34_0_active_min_day_diff
,datediff(date_dt,tgi1_25_34_5_active_min_day) as tgi1_25_34_5_active_min_day_diff
,datediff(date_dt,tgi1_45_54_0_active_min_day) as tgi1_45_54_0_active_min_day_diff
,datediff(date_dt,tgi2_18_24_5_active_min_day) as tgi2_18_24_5_active_min_day_diff
,datediff(date_dt,tgi2_35_44_6_active_min_day) as tgi2_35_44_6_active_min_day_diff
,datediff(date_dt,tgi2_45_54_5_active_min_day) as tgi2_45_54_5_active_min_day_diff
from(
    select device
  ,max(case when cate_id='cate17' then max_day else null end ) as cate17_active_max_day
,max(case when cate_id='cate7001_002' then max_day else null end ) as cate7001_002_active_max_day
,max(case when cate_id='cate7005_001' then max_day else null end ) as cate7005_001_active_max_day
,max(case when cate_id='cate7010_001' then max_day else null end ) as cate7010_001_active_max_day
,max(case when cate_id='cate7011_998' then max_day else null end ) as cate7011_998_active_max_day
,max(case when cate_id='cate7013_008' then max_day else null end ) as cate7013_008_active_max_day
,max(case when cate_id='cate7015_001' then max_day else null end ) as cate7015_001_active_max_day
,max(case when cate_id='fin_46' then max_day else null end ) as fin_46_active_max_day
,max(case when cate_id='tgi1_18_0' then max_day else null end ) as tgi1_18_0_active_max_day
,max(case when cate_id='tgi1_35_44_1' then max_day else null end ) as tgi1_35_44_1_active_max_day
,max(case when cate_id='tgi1_35_44_5' then max_day else null end ) as tgi1_35_44_5_active_max_day
,max(case when cate_id='tgi1_45_54_5' then max_day else null end ) as tgi1_45_54_5_active_max_day
,max(case when cate_id='tgi2_18_0' then max_day else null end ) as tgi2_18_0_active_max_day
,max(case when cate_id='tgi2_18_24_5' then max_day else null end ) as tgi2_18_24_5_active_max_day
,max(case when cate_id='tgi2_25_34_5' then max_day else null end ) as tgi2_25_34_5_active_max_day
,max(case when cate_id='tgi2_35_44_5' then max_day else null end ) as tgi2_35_44_5_active_max_day
,max(case when cate_id='tgi2_35_44_6' then max_day else null end ) as tgi2_35_44_6_active_max_day
,max(case when cate_id='tgi2_45_54_1' then max_day else null end ) as tgi2_45_54_1_active_max_day
,max(case when cate_id='tgi2_55_0' then max_day else null end ) as tgi2_55_0_active_max_day
,max(case when cate_id='tgi2_55_1' then max_day else null end ) as tgi2_55_1_active_max_day
,min(case when cate_id='cate2' then min_day else null end ) as cate2_active_min_day
,min(case when cate_id='cate22' then min_day else null end ) as cate22_active_min_day
,min(case when cate_id='cate7011_993' then min_day else null end ) as cate7011_993_active_min_day
,min(case when cate_id='cate7012_012' then min_day else null end ) as cate7012_012_active_min_day
,min(case when cate_id='cate7019_107' then min_day else null end ) as cate7019_107_active_min_day
,min(case when cate_id='tgi1_18_24_0' then min_day else null end ) as tgi1_18_24_0_active_min_day
,min(case when cate_id='tgi1_18_6' then min_day else null end ) as tgi1_18_6_active_min_day
,min(case when cate_id='tgi1_25_34_0' then min_day else null end ) as tgi1_25_34_0_active_min_day
,min(case when cate_id='tgi1_25_34_5' then min_day else null end ) as tgi1_25_34_5_active_min_day
,min(case when cate_id='tgi1_45_54_0' then min_day else null end ) as tgi1_45_54_0_active_min_day
,min(case when cate_id='tgi2_18_24_5' then min_day else null end ) as tgi2_18_24_5_active_min_day
,min(case when cate_id='tgi2_35_44_6' then min_day else null end ) as tgi2_35_44_6_active_min_day
,min(case when cate_id='tgi2_45_54_5' then min_day else null end ) as tgi2_45_54_5_active_min_day
,date_dt
    from(
        select device,a.cate_id,date_dt,min(day_dt) as min_day,max(day_dt) as max_day
        from
        (
            select pkg, cate_id
            from $dim_age_app_category_final_new
            where cate_id in ('cate22'
                    ,'tgi2_45_54_1'
                    ,'cate7001_002'
                    ,'cate7019_107'
                    ,'tgi1_45_54_0'
                    ,'tgi1_45_54_5'
                    ,'cate2'
                    ,'tgi2_35_44_6'
                    ,'cate17'
                    ,'tgi2_25_34_5'
                    ,'cate7010_001'
                    ,'cate7012_012'
                    ,'tgi2_45_54_5'
                    ,'tgi1_25_34_0'
                    ,'tgi1_18_6'
                    ,'tgi2_55_0'
                    ,'cate7011_993'
                    ,'cate7015_001'
                    ,'tgi1_35_44_5'
                    ,'cate7011_998'
                    ,'tgi2_35_44_5'
                    ,'fin_46'
                    ,'tgi1_25_34_5'
                    ,'tgi2_18_24_5'
                    ,'tgi1_18_0'
                    ,'cate7005_001'
                    ,'cate7013_008'
                    ,'tgi2_18_0'
                    ,'tgi1_18_24_0'
                    ,'tgi2_55_1'
                    ,'tgi1_35_44_1')
            group by pkg, cate_id
        )a 
        join
        (
                select device,pkg,day
                ,from_unixtime(unix_timestamp('$day','yyyyMMdd'),'yyyy-MM-dd') as date_dt
                ,from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd') as day_dt
                from $dws_device_active_applist_di
                where day between $day_before_one_year and $day
        )b
        on a.pkg=b.pkg
        group by device,a.cate_id,date_dt
    )t1 
    group by device,date_dt
)t2;
"