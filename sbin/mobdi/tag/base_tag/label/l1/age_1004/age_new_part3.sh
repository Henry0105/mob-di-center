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

age_new_pkg_install_12="${dm_mobdi_tmp}.age_new_pkg_install_12"



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
insert overwrite table $age_new_pkg_install_12 partition (day='$insertday')
select device
,max(case when cate_id='cate14' then cnt_1 else 0 end ) as cate14_12_12
,max(case when cate_id='cate26' then cnt_1 else 0 end ) as cate26_12_12
,max(case when cate_id='cate7002_007' then cnt_1 else 0 end ) as cate7002_007_12_12
,max(case when cate_id='cate7006_001' then cnt_1 else 0 end ) as cate7006_001_12_12
,max(case when cate_id='cate7008_010' then cnt_1 else 0 end ) as cate7008_010_12_12
,max(case when cate_id='cate7009_001' then cnt_1 else 0 end ) as cate7009_001_12_12
,max(case when cate_id='cate7010_001' then cnt_1 else 0 end ) as cate7010_001_12_12
,max(case when cate_id='cate7011_993' then cnt_1 else 0 end ) as cate7011_993_12_12
,max(case when cate_id='cate7014_002' then cnt_1 else 0 end ) as cate7014_002_12_12
,max(case when cate_id='cate7019_122' then cnt_1 else 0 end ) as cate7019_122_12_12
,max(case when cate_id='cate7019_127' then cnt_1 else 0 end ) as cate7019_127_12_12
,max(case when cate_id='tgi1_18_1' then cnt_1 else 0 end ) as tgi1_18_1_12_12
,max(case when cate_id='tgi1_18_2' then cnt_1 else 0 end ) as tgi1_18_2_12_12
,max(case when cate_id='tgi1_25_34_4' then cnt_1 else 0 end ) as tgi1_25_34_4_12_12
,max(case when cate_id='tgi1_35_44_5' then cnt_1 else 0 end ) as tgi1_35_44_5_12_12
,max(case when cate_id='tgi2_18_0' then cnt_1 else 0 end ) as tgi2_18_0_12_12
,max(case when cate_id='tgi2_18_24_2' then cnt_1 else 0 end ) as tgi2_18_24_2_12_12
,max(case when cate_id='tgi2_18_24_4' then cnt_1 else 0 end ) as tgi2_18_24_4_12_12
,max(case when cate_id='tgi2_18_7' then cnt_1 else 0 end ) as tgi2_18_7_12_12
,max(case when cate_id='tgi2_25_34_5' then cnt_1 else 0 end ) as tgi2_25_34_5_12_12
,max(case when cate_id='tgi2_25_34_6' then cnt_1 else 0 end ) as tgi2_25_34_6_12_12
,max(case when cate_id='tgi2_35_44_4' then cnt_1 else 0 end ) as tgi2_35_44_4_12_12
,max(case when cate_id='tgi2_35_44_6' then cnt_1 else 0 end ) as tgi2_35_44_6_12_12

from
(
    select device,cate_id,count(pkg)/12 as cnt_1 
    from 
    (
        select device,a1.pkg,cate_id
        from(
            select * 
            from $dim_age_app_category_final_new
            where cate_id in (
                'cate14',
                'cate26',
                'cate7002_007',
                'cate7006_001',
                'cate7008_010',
                'cate7009_001',
                'cate7010_001',
                'cate7011_993',
                'cate7014_002',
                'cate7019_122',
                'cate7019_127',
                'tgi1_18_1',
                'tgi1_18_2',
                'tgi1_25_34_4',
                'tgi1_35_44_5',
                'tgi2_18_0',
                'tgi2_18_24_2',
                'tgi2_18_24_4',
                'tgi2_18_7',
                'tgi2_25_34_5',
                'tgi2_25_34_6',
                'tgi2_35_44_4',
                'tgi2_35_44_6'
                )
        )a1 inner join 
        (
            select * from $label_device_pkg_install_uninstall_year_info_mf
            where day=$day and refine_final_flag in (0,1) 
            and update_day between $day_before_one_month and $day
        )a2
        on a1.pkg = a2.pkg
        group by device,a1.pkg,cate_id
    )a
    group by device,cate_id
)t
group by device;
"