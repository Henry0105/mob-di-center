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

age_new_applist_install_bycate_id_whether="${dm_mobdi_tmp}.age_new_applist_install_bycate_id_whether"

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
insert overwrite table $age_new_applist_install_bycate_id_whether partition (day='$insertday')
select device
,sum(case when datediff(date,update_day)<=30 then 1 else 0 end ) as flag_1m
,sum(case when datediff(date,update_day)<=30 and cate_id='tgi2_55_0' then 1 else 0 end ) as flag_1m_tgi2_55_0
,sum(case when datediff(date,update_day)<=90 then 1 else 0 end ) as flag_3m
,sum(case when datediff(date,update_day)<=90 and cate_id='tgi2_18_24_0' then 1 else 0 end ) as flag_3m_tgi2_18_24_0
,sum(case when datediff(date,update_day)<=90 and cate_id='tgi2_55_0' then 1 else 0 end ) as flag_3m_tgi2_55_0
,sum(case when datediff(date,update_day)<=180 then 1 else 0 end ) as flag_6m
,sum(case when datediff(date,update_day)<=180 and cate_id='tgi2_18_0' then 1 else 0 end ) as flag_6m_tgi2_18_0
,sum(case when datediff(date,update_day)<=365 then 1 else 0 end ) as flag_12m
,sum(case when datediff(date,update_day)<=365 and cate_id='cate17' then 1 else 0 end ) as flag_12m_cate17
,sum(case when datediff(date,update_day)<=365 and cate_id='cate2' then 1 else 0 end ) as flag_12m_cate2
,sum(case when datediff(date,update_day)<=365 and cate_id='cate22' then 1 else 0 end ) as flag_12m_cate22
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7001_002' then 1 else 0 end ) as flag_12m_cate7001_002
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7005_001' then 1 else 0 end ) as flag_12m_cate7005_001
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7010_001' then 1 else 0 end ) as flag_12m_cate7010_001
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7011_993' then 1 else 0 end ) as flag_12m_cate7011_993
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7011_998' then 1 else 0 end ) as flag_12m_cate7011_998
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7012_012' then 1 else 0 end ) as flag_12m_cate7012_012
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7013_008' then 1 else 0 end ) as flag_12m_cate7013_008
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7015_001' then 1 else 0 end ) as flag_12m_cate7015_001
,sum(case when datediff(date,update_day)<=365 and cate_id='cate7019_107' then 1 else 0 end ) as flag_12m_cate7019_107
,sum(case when datediff(date,update_day)<=365 and cate_id='fin_46' then 1 else 0 end ) as flag_12m_fin_46
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_18_0' then 1 else 0 end ) as flag_12m_tgi1_18_0
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_18_24_0' then 1 else 0 end ) as flag_12m_tgi1_18_24_0
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_18_6' then 1 else 0 end ) as flag_12m_tgi1_18_6
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_25_34_0' then 1 else 0 end ) as flag_12m_tgi1_25_34_0
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_25_34_5' then 1 else 0 end ) as flag_12m_tgi1_25_34_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_35_44_1' then 1 else 0 end ) as flag_12m_tgi1_35_44_1
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_35_44_5' then 1 else 0 end ) as flag_12m_tgi1_35_44_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_45_54_0' then 1 else 0 end ) as flag_12m_tgi1_45_54_0
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi1_45_54_5' then 1 else 0 end ) as flag_12m_tgi1_45_54_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_18_0' then 1 else 0 end ) as flag_12m_tgi2_18_0
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_18_24_5' then 1 else 0 end ) as flag_12m_tgi2_18_24_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_18_5' then 1 else 0 end ) as flag_12m_tgi2_18_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_18_6' then 1 else 0 end ) as flag_12m_tgi2_18_6
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_25_34_5' then 1 else 0 end ) as flag_12m_tgi2_25_34_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_35_44_5' then 1 else 0 end ) as flag_12m_tgi2_35_44_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_35_44_6' then 1 else 0 end ) as flag_12m_tgi2_35_44_6
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_45_54_1' then 1 else 0 end ) as flag_12m_tgi2_45_54_1
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_45_54_5' then 1 else 0 end ) as flag_12m_tgi2_45_54_5
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_55_0' then 1 else 0 end ) as flag_12m_tgi2_55_0
,sum(case when datediff(date,update_day)<=365 and cate_id='tgi2_55_1' then 1 else 0 end ) as flag_12m_tgi2_55_1
from(
        select device,b1.pkg,cate_id,refine_final_flag,date,update_day
        from(
            select pkg, cate_id
            from $dim_age_app_category_final_new
            group by pkg, cate_id
        )b1
        inner join(
             select device,pkg,refine_final_flag
            ,from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd') as date
            ,from_unixtime(unix_timestamp(update_day,'yyyyMMdd'),'yyyy-MM-dd') as update_day
            from $label_device_pkg_install_uninstall_year_info_mf
            where day=$day and update_day between $day_before_one_month and $day
            group by device,pkg,refine_final_flag,update_day,day 
        )b2 
        on b1.pkg = b2.pkg
)b 
group by device;
"