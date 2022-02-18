#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")
insertday=${day}_muid

source /home/dba/mobdi_center/conf/hive-env.sh

#dim_age_app_category_final_new=dim_mobdi_mapping.dim_age_app_category_final_new
#category_mapping_table=dm_mobdi_mapping.age_app_category_final_new
#label_device_pkg_install_uninstall_year_info_mf=rp_mobdi_app.label_device_pkg_install_uninstall_year_info_mf
tmpdb=$dm_mobdi_tmp
age_new_ratio_features_12m="${tmpdb}.age_new_ratio_features_12m"
age_new_ratio_features_6m="${tmpdb}.age_new_ratio_features_6m"
age_new_install_cnt_6mv12m="${tmpdb}.age_new_install_cnt_6mv12m"


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
insert overwrite table $age_new_ratio_features_12m partition(day='$insertday')
select device
,cate2_12m_cnt
,cate7006_001_12m_cnt
,cate7015_005_12m_cnt
,cate7016_001_12m_cnt
,cate7016_002_12m_cnt
,fin_46_12m_cnt
,tgi1_18_0_12m_cnt
,tgi1_25_34_1_12m_cnt
,tgi1_55_0_12m_cnt
,tgi2_35_44_6_12m_cnt

,cate26_12m_cnt
,cate7002_001_12m_cnt
,cate7005_001_12m_cnt
,cate7005_002_12m_cnt
,cate7007_001_12m_cnt
,cate7009_001_12m_cnt
,cate7014_007_12m_cnt
,cate7019_122_12m_cnt
,cate7019_127_12m_cnt
,tgi1_18_1_12m_cnt
,tgi1_18_24_6_12m_cnt
,tgi1_25_34_4_12m_cnt
,tgi1_35_44_0_12m_cnt
,tgi2_18_2_12m_cnt
,tgi2_18_24_2_12m_cnt
,tgi2_45_54_0_12m_cnt
,cate14_12m_ratio
,cate7001_003_12m_ratio
,cate7001_006_12m_ratio
,cate7001_013_12m_ratio
,cate7002_005_12m_ratio
,cate7002_007_12m_ratio
,cate7003_001_12m_ratio
,cate7005_001_12m_ratio
,cate7005_002_12m_ratio
,cate7006_001_12m_ratio
,cate7006_003_12m_ratio
,cate7008_003_12m_ratio
,cate7009_001_12m_ratio
,cate7010_002_12m_ratio
,cate7011_995_12m_ratio
,cate7011_997_12m_ratio
,cate7012_003_12m_ratio
,cate7012_007_12m_ratio
,cate7014_009_12m_ratio
,cate7014_010_12m_ratio
,cate7014_017_12m_ratio
,cate7015_004_12m_ratio
,cate7015_006_12m_ratio
,cate7015_019_12m_ratio
,cate7016_002_12m_ratio
,cate7016_003_12m_ratio
,cate7019_136_12m_ratio
,fin_27_12m_ratio
,tgi1_18_0_12m_ratio
,tgi1_18_2_12m_ratio
,tgi1_18_24_3_12m_ratio
,tgi1_35_44_1_12m_ratio
,tgi1_55_6_12m_ratio
,tgi2_18_1_12m_ratio
,tgi1_18_6_12m_ratio
,tgi2_18_24_0_12m_ratio
,tgi2_18_24_1_12m_ratio
,tgi2_18_24_2_12m_ratio
,tgi2_18_24_4_12m_ratio
,tgi2_18_4_12m_ratio
,tgi2_18_5_12m_ratio
,tgi2_18_7_12m_ratio
,tgi2_25_34_3_12m_ratio
,tgi2_25_34_4_12m_ratio
,tgi2_25_34_5_12m_ratio
,tgi2_25_34_6_12m_ratio
,tgi2_35_44_2_12m_ratio
,tgi2_35_44_3_12m_ratio
,tgi2_35_44_4_12m_ratio
,tgi2_35_44_5_12m_ratio
,tgi2_45_54_2_12m_ratio
,tgi2_45_54_3_12m_ratio
,tgi2_45_54_4_12m_ratio
,tgi2_45_54_5_12m_ratio
,tgi2_55_2_12m_ratio
,tgi2_55_3_12m_ratio
,tgi2_55_4_12m_ratio
,tgi2_55_5_12m_ratio
,tgi2_55_7_12m_ratio
from(
select
    *
,round(COALESCE(cate14_12m_cnt*1.000000/total_12m_cnt,0),6) as cate14_12m_ratio
,round(COALESCE(cate7001_003_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7001_003_12m_ratio
,round(COALESCE(cate7001_006_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7001_006_12m_ratio
,round(COALESCE(cate7001_013_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7001_013_12m_ratio
,round(COALESCE(cate7002_005_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7002_005_12m_ratio
,round(COALESCE(cate7002_007_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7002_007_12m_ratio
,round(COALESCE(cate7003_001_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7003_001_12m_ratio
,round(COALESCE(cate7005_001_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7005_001_12m_ratio
,round(COALESCE(cate7005_002_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7005_002_12m_ratio
,round(COALESCE(cate7006_001_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7006_001_12m_ratio
,round(COALESCE(cate7006_003_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7006_003_12m_ratio
,round(COALESCE(cate7008_003_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7008_003_12m_ratio
,round(COALESCE(cate7009_001_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7009_001_12m_ratio
,round(COALESCE(cate7010_002_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7010_002_12m_ratio
,round(COALESCE(cate7011_995_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7011_995_12m_ratio
,round(COALESCE(cate7011_997_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7011_997_12m_ratio
,round(COALESCE(cate7012_003_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7012_003_12m_ratio
,round(COALESCE(cate7012_007_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7012_007_12m_ratio
,round(COALESCE(cate7014_009_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7014_009_12m_ratio
,round(COALESCE(cate7014_010_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7014_010_12m_ratio
,round(COALESCE(cate7014_017_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7014_017_12m_ratio
,round(COALESCE(cate7015_004_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7015_004_12m_ratio
,round(COALESCE(cate7015_006_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7015_006_12m_ratio
,round(COALESCE(cate7015_019_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7015_019_12m_ratio
,round(COALESCE(cate7016_002_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7016_002_12m_ratio
,round(COALESCE(cate7016_003_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7016_003_12m_ratio
,round(COALESCE(cate7019_136_12m_cnt*1.000000/total_12m_cnt,0),6) as cate7019_136_12m_ratio
,round(COALESCE(fin_27_12m_cnt*1.000000/total_12m_cnt,0),6) as fin_27_12m_ratio
,round(COALESCE(tgi1_18_0_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi1_18_0_12m_ratio
,round(COALESCE(tgi1_18_2_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi1_18_2_12m_ratio
,round(COALESCE(tgi1_18_24_3_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi1_18_24_3_12m_ratio
,round(COALESCE(tgi1_35_44_1_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi1_35_44_1_12m_ratio
,round(COALESCE(tgi1_55_6_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi1_55_6_12m_ratio
,round(COALESCE(tgi2_18_1_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_1_12m_ratio
,round(COALESCE(tgi1_18_6_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi1_18_6_12m_ratio

,round(COALESCE(tgi2_18_24_0_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_24_0_12m_ratio
,round(COALESCE(tgi2_18_24_1_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_24_1_12m_ratio
,round(COALESCE(tgi2_18_24_2_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_24_2_12m_ratio
,round(COALESCE(tgi2_18_24_4_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_24_4_12m_ratio

,round(COALESCE(tgi2_18_4_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_4_12m_ratio
,round(COALESCE(tgi2_18_5_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_5_12m_ratio
,round(COALESCE(tgi2_18_7_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_18_7_12m_ratio

,round(COALESCE(tgi2_25_34_3_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_25_34_3_12m_ratio
,round(COALESCE(tgi2_25_34_4_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_25_34_4_12m_ratio
,round(COALESCE(tgi2_25_34_5_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_25_34_5_12m_ratio
,round(COALESCE(tgi2_25_34_6_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_25_34_6_12m_ratio

,round(COALESCE(tgi2_35_44_2_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_35_44_2_12m_ratio
,round(COALESCE(tgi2_35_44_3_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_35_44_3_12m_ratio
,round(COALESCE(tgi2_35_44_4_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_35_44_4_12m_ratio
,round(COALESCE(tgi2_35_44_5_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_35_44_5_12m_ratio

,round(COALESCE(tgi2_45_54_2_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_45_54_2_12m_ratio
,round(COALESCE(tgi2_45_54_3_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_45_54_3_12m_ratio
,round(COALESCE(tgi2_45_54_4_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_45_54_4_12m_ratio
,round(COALESCE(tgi2_45_54_5_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_45_54_5_12m_ratio

,round(COALESCE(tgi2_55_2_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_55_2_12m_ratio
,round(COALESCE(tgi2_55_3_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_55_3_12m_ratio
,round(COALESCE(tgi2_55_4_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_55_4_12m_ratio
,round(COALESCE(tgi2_55_5_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_55_5_12m_ratio
,round(COALESCE(tgi2_55_7_12m_cnt*1.000000/total_12m_cnt,0),6) as tgi2_55_7_12m_ratio
from(
        select device
,count( 1  ) as total_12m_cnt
,count(case when cate_id='cate2' then 1 else null end) as cate2_12m_cnt
,count(case when cate_id='cate7006_001' then 1 else null end) as cate7006_001_12m_cnt
,count(case when cate_id='cate7015_005' then 1 else null end) as cate7015_005_12m_cnt
,count(case when cate_id='cate7016_001' then 1 else null end) as cate7016_001_12m_cnt
,count(case when cate_id='cate7016_002' then 1 else null end) as cate7016_002_12m_cnt
,count(case when cate_id='fin_46' then 1 else null end) as fin_46_12m_cnt
,count(case when cate_id='tgi1_18_0' then 1 else null end) as tgi1_18_0_12m_cnt
,count(case when cate_id='tgi1_25_34_1' then 1 else null end) as tgi1_25_34_1_12m_cnt
,count(case when cate_id='tgi1_55_0' then 1 else null end) as tgi1_55_0_12m_cnt
,count(case when cate_id='tgi2_35_44_6' then 1 else null end) as tgi2_35_44_6_12m_cnt
,count(case when cate_id='cate14' then 1 else null end) as cate14_12m_cnt
,count(case when cate_id='cate26' then 1 else null end) as cate26_12m_cnt
,count(case when cate_id='cate7001_003' then 1 else null end) as cate7001_003_12m_cnt
,count(case when cate_id='cate7001_006' then 1 else null end) as cate7001_006_12m_cnt
,count(case when cate_id='cate7001_013' then 1 else null end) as cate7001_013_12m_cnt
,count(case when cate_id='cate7002_001' then 1 else null end) as cate7002_001_12m_cnt
,count(case when cate_id='cate7002_005' then 1 else null end) as cate7002_005_12m_cnt
,count(case when cate_id='cate7002_007' then 1 else null end) as cate7002_007_12m_cnt
,count(case when cate_id='cate7003_001' then 1 else null end) as cate7003_001_12m_cnt
,count(case when cate_id='cate7005_001' then 1 else null end) as cate7005_001_12m_cnt
,count(case when cate_id='cate7005_002' then 1 else null end) as cate7005_002_12m_cnt
,count(case when cate_id='cate7006_003' then 1 else null end) as cate7006_003_12m_cnt
,count(case when cate_id='cate7007_001' then 1 else null end) as cate7007_001_12m_cnt
,count(case when cate_id='cate7008_003' then 1 else null end) as cate7008_003_12m_cnt
,count(case when cate_id='cate7009_001' then 1 else null end) as cate7009_001_12m_cnt
,count(case when cate_id='cate7010_002' then 1 else null end) as cate7010_002_12m_cnt
,count(case when cate_id='cate7011_995' then 1 else null end) as cate7011_995_12m_cnt
,count(case when cate_id='cate7011_997' then 1 else null end) as cate7011_997_12m_cnt
,count(case when cate_id='cate7012_003' then 1 else null end) as cate7012_003_12m_cnt
,count(case when cate_id='cate7012_007' then 1 else null end) as cate7012_007_12m_cnt
,count(case when cate_id='cate7014_007' then 1 else null end) as cate7014_007_12m_cnt
,count(case when cate_id='cate7014_009' then 1 else null end) as cate7014_009_12m_cnt
,count(case when cate_id='cate7014_010' then 1 else null end) as cate7014_010_12m_cnt
,count(case when cate_id='cate7014_017' then 1 else null end) as cate7014_017_12m_cnt
,count(case when cate_id='cate7015_004' then 1 else null end) as cate7015_004_12m_cnt
,count(case when cate_id='cate7015_006' then 1 else null end) as cate7015_006_12m_cnt
,count(case when cate_id='cate7015_019' then 1 else null end) as cate7015_019_12m_cnt
,count(case when cate_id='cate7016_003' then 1 else null end) as cate7016_003_12m_cnt
,count(case when cate_id='cate7019_122' then 1 else null end) as cate7019_122_12m_cnt
,count(case when cate_id='cate7019_127' then 1 else null end) as cate7019_127_12m_cnt
,count(case when cate_id='cate7019_136' then 1 else null end) as cate7019_136_12m_cnt
,count(case when cate_id='fin_27' then 1 else null end) as fin_27_12m_cnt
,count(case when cate_id='tgi1_18_1' then 1 else null end) as tgi1_18_1_12m_cnt
,count(case when cate_id='tgi1_18_2' then 1 else null end) as tgi1_18_2_12m_cnt
,count(case when cate_id='tgi1_18_24_3' then 1 else null end) as tgi1_18_24_3_12m_cnt
,count(case when cate_id='tgi1_18_24_6' then 1 else null end) as tgi1_18_24_6_12m_cnt
,count(case when cate_id='tgi1_18_6' then 1 else null end) as tgi1_18_6_12m_cnt
,count(case when cate_id='tgi1_25_34_4' then 1 else null end) as tgi1_25_34_4_12m_cnt
,count(case when cate_id='tgi1_35_44_0' then 1 else null end) as tgi1_35_44_0_12m_cnt
,count(case when cate_id='tgi1_35_44_1' then 1 else null end) as tgi1_35_44_1_12m_cnt
,count(case when cate_id='tgi1_55_6' then 1 else null end) as tgi1_55_6_12m_cnt
,count(case when cate_id='tgi2_18_1' then 1 else null end) as tgi2_18_1_12m_cnt
,count(case when cate_id='tgi2_18_2' then 1 else null end) as tgi2_18_2_12m_cnt
,count(case when cate_id='tgi2_18_24_0' then 1 else null end) as tgi2_18_24_0_12m_cnt
,count(case when cate_id='tgi2_18_24_1' then 1 else null end) as tgi2_18_24_1_12m_cnt
,count(case when cate_id='tgi2_18_24_2' then 1 else null end) as tgi2_18_24_2_12m_cnt

,count(case when cate_id='tgi2_18_24_4' then 1 else null end) as tgi2_18_24_4_12m_cnt
,count(case when cate_id='tgi2_18_4' then 1 else null end) as tgi2_18_4_12m_cnt
,count(case when cate_id='tgi2_18_5' then 1 else null end) as tgi2_18_5_12m_cnt
,count(case when cate_id='tgi2_18_7' then 1 else null end) as tgi2_18_7_12m_cnt
,count(case when cate_id='tgi2_25_34_3' then 1 else null end) as tgi2_25_34_3_12m_cnt
,count(case when cate_id='tgi2_25_34_4' then 1 else null end) as tgi2_25_34_4_12m_cnt
,count(case when cate_id='tgi2_25_34_5' then 1 else null end) as tgi2_25_34_5_12m_cnt
,count(case when cate_id='tgi2_25_34_6' then 1 else null end) as tgi2_25_34_6_12m_cnt

,count(case when cate_id='tgi2_35_44_2' then 1 else null end) as tgi2_35_44_2_12m_cnt
,count(case when cate_id='tgi2_35_44_3' then 1 else null end) as tgi2_35_44_3_12m_cnt
,count(case when cate_id='tgi2_35_44_4' then 1 else null end) as tgi2_35_44_4_12m_cnt
,count(case when cate_id='tgi2_35_44_5' then 1 else null end) as tgi2_35_44_5_12m_cnt
,count(case when cate_id='tgi2_45_54_0' then 1 else null end) as tgi2_45_54_0_12m_cnt
,count(case when cate_id='tgi2_45_54_2' then 1 else null end) as tgi2_45_54_2_12m_cnt
,count(case when cate_id='tgi2_45_54_3' then 1 else null end) as tgi2_45_54_3_12m_cnt
,count(case when cate_id='tgi2_45_54_4' then 1 else null end) as tgi2_45_54_4_12m_cnt
,count(case when cate_id='tgi2_45_54_5' then 1 else null end) as tgi2_45_54_5_12m_cnt
,count(case when cate_id='tgi2_55_2' then 1 else null end) as tgi2_55_2_12m_cnt
,count(case when cate_id='tgi2_55_3' then 1 else null end) as tgi2_55_3_12m_cnt
,count(case when cate_id='tgi2_55_4' then 1 else null end) as tgi2_55_4_12m_cnt
,count(case when cate_id='tgi2_55_5' then 1 else null end) as tgi2_55_5_12m_cnt
,count(case when cate_id='tgi2_55_7' then 1 else null end) as tgi2_55_7_12m_cnt
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
         where datediff(date,update_day)<=365 and  datediff(date,flag_day)<=365
        group by device
    )t2 
)t3;
"




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
insert overwrite table $age_new_ratio_features_6m partition(day='$insertday')
select
    device
    ,cate2_6m_cnt
    ,cate7006_001_6m_cnt
    ,cate7015_005_6m_cnt
    ,cate7016_001_6m_cnt
    ,cate7016_002_6m_cnt
    ,cate7019_122_6m_cnt
    ,fin_46_6m_cnt
    ,tgi1_25_34_1_6m_cnt
    ,tgi1_55_0_6m_cnt
    ,tgi2_35_44_6_6m_cnt
    ,tgi2_45_54_0_6m_cnt
    ,tgi1_18_0_6m_cnt
    ,tgi1_18_24_6_6m_cnt
    ,round(COALESCE(cate22_6m_cnt*1.000000/total_6m_cnt,0),6) as cate22_6m_ratio
    ,round(COALESCE(cate7001_003_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7001_003_6m_ratio
    ,round(COALESCE(cate7002_003_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7002_003_6m_ratio
    ,round(COALESCE(cate7002_005_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7002_005_6m_ratio
    ,round(COALESCE(cate7003_001_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7003_001_6m_ratio
    ,round(COALESCE(cate7005_002_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7005_002_6m_ratio
    ,round(COALESCE(cate7006_003_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7006_003_6m_ratio
    ,round(COALESCE(cate7012_001_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7012_001_6m_ratio
    ,round(COALESCE(cate7012_003_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7012_003_6m_ratio
    ,round(COALESCE(cate7012_013_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7012_013_6m_ratio
    ,round(COALESCE(cate7012_014_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7012_014_6m_ratio
    ,round(COALESCE(cate7014_009_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7014_009_6m_ratio
    ,round(COALESCE(cate7014_010_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7014_010_6m_ratio
    ,round(COALESCE(cate7014_017_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7014_017_6m_ratio
    ,round(COALESCE(cate7015_004_6m_cnt*1.000000/total_6m_cnt,0),6) as cate7015_004_6m_ratio
    ,round(COALESCE(fin_27_6m_cnt*1.000000/total_6m_cnt,0),6) as fin_27_6m_ratio
    ,round(COALESCE(tgi1_18_0_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_18_0_6m_ratio
    ,round(COALESCE(tgi2_18_24_0_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_18_24_0_6m_ratio
    ,round(COALESCE(tgi2_18_24_1_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_18_24_1_6m_ratio
    ,round(COALESCE(tgi2_18_4_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_18_4_6m_ratio
    ,round(COALESCE(tgi2_25_34_3_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_25_34_3_6m_ratio
    ,round(COALESCE(tgi2_25_34_6_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_25_34_6_6m_ratio
    ,round(COALESCE(tgi2_35_44_3_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_35_44_3_6m_ratio
    ,round(COALESCE(tgi2_35_44_5_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_35_44_5_6m_ratio
    ,round(COALESCE(tgi2_55_2_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi2_55_2_6m_ratio
from(
        select 
              device
        ,count( pkg) as total_6m_cnt
        ,count(case when cate_id='cate2' then 1 else null end) as cate2_6m_cnt
        ,count(case when cate_id='cate7006_001' then 1 else null end) as cate7006_001_6m_cnt
        ,count(case when cate_id='cate7015_005' then 1 else null end) as cate7015_005_6m_cnt
        ,count(case when cate_id='cate7016_001' then 1 else null end) as cate7016_001_6m_cnt
        ,count(case when cate_id='cate7016_002' then 1 else null end) as cate7016_002_6m_cnt
        ,count(case when cate_id='cate7019_122' then 1 else null end) as cate7019_122_6m_cnt
        ,count(case when cate_id='fin_46' then 1 else null end) as fin_46_6m_cnt
        ,count(case when cate_id='tgi1_25_34_1' then 1 else null end) as tgi1_25_34_1_6m_cnt
        ,count(case when cate_id='tgi1_55_0' then 1 else null end) as tgi1_55_0_6m_cnt
        ,count(case when cate_id='tgi2_35_44_6' then 1 else null end) as tgi2_35_44_6_6m_cnt
        ,count(case when cate_id='tgi2_45_54_0' then 1 else null end) as tgi2_45_54_0_6m_cnt

        ,count(case when cate_id='cate22' then 1 else null end) as cate22_6m_cnt
        ,count(case when cate_id='cate7001_003' then 1 else null end) as cate7001_003_6m_cnt
        ,count(case when cate_id='cate7002_003' then 1 else null end) as cate7002_003_6m_cnt
        ,count(case when cate_id='cate7002_005' then 1 else null end) as cate7002_005_6m_cnt
        ,count(case when cate_id='cate7003_001' then 1 else null end) as cate7003_001_6m_cnt
        ,count(case when cate_id='cate7005_002' then 1 else null end) as cate7005_002_6m_cnt
        ,count(case when cate_id='cate7006_003' then 1 else null end) as cate7006_003_6m_cnt
        ,count(case when cate_id='cate7012_001' then 1 else null end) as cate7012_001_6m_cnt
        ,count(case when cate_id='cate7012_003' then 1 else null end) as cate7012_003_6m_cnt
        ,count(case when cate_id='cate7012_013' then 1 else null end) as cate7012_013_6m_cnt
        ,count(case when cate_id='cate7012_014' then 1 else null end) as cate7012_014_6m_cnt
        ,count(case when cate_id='cate7014_009' then 1 else null end) as cate7014_009_6m_cnt
        ,count(case when cate_id='cate7014_010' then 1 else null end) as cate7014_010_6m_cnt
        ,count(case when cate_id='cate7014_017' then 1 else null end) as cate7014_017_6m_cnt
        ,count(case when cate_id='cate7015_004' then 1 else null end) as cate7015_004_6m_cnt
        ,count(case when cate_id='fin_27' then 1 else null end) as fin_27_6m_cnt
        ,count(case when cate_id='tgi1_18_0' then 1 else null end) as tgi1_18_0_6m_cnt
        ,count(case when cate_id='tgi1_18_24_6' then 1 else null end) as tgi1_18_24_6_6m_cnt
        ,count(case when cate_id='tgi2_18_24_0' then 1 else null end) as tgi2_18_24_0_6m_cnt
        ,count(case when cate_id='tgi2_18_24_1' then 1 else null end) as tgi2_18_24_1_6m_cnt
        ,count(case when cate_id='tgi2_18_4' then 1 else null end) as tgi2_18_4_6m_cnt
        ,count(case when cate_id='tgi2_18_5' then 1 else null end) as tgi2_18_5_6m_cnt
        ,count(case when cate_id='tgi2_18_6' then 1 else null end) as tgi2_18_6_6m_cnt
        ,count(case when cate_id='tgi2_18_7' then 1 else null end) as tgi2_18_7_6m_cnt
        ,count(case when cate_id='tgi2_25_34_3' then 1 else null end) as tgi2_25_34_3_6m_cnt
        ,count(case when cate_id='tgi2_25_34_6' then 1 else null end) as tgi2_25_34_6_6m_cnt
        ,count(case when cate_id='tgi2_35_44_3' then 1 else null end) as tgi2_35_44_3_6m_cnt
        ,count(case when cate_id='tgi2_35_44_5' then 1 else null end) as tgi2_35_44_5_6m_cnt
        ,count(case when cate_id='tgi2_55_2' then 1 else null end) as tgi2_55_2_6m_cnt

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
                ,day
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
        where datediff(date,update_day)<=180 and datediff(date,flag_day)<=180
        group by device
    )t2 ;
"


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
insert overwrite table $age_new_install_cnt_6mv12m partition (day='$insertday')
select a.device
,case when cate2_6m_cnt=-999 or cate2_12m_cnt=-999 then -999 when cate2_12m_cnt=0 then -9 else cate2_6m_cnt/cate2_12m_cnt end as cate2_6mv12m
,case when cate7006_001_6m_cnt=-999 or cate7006_001_12m_cnt=-999 then -999 when cate7006_001_12m_cnt=0 then -9 else cate7006_001_6m_cnt/cate7006_001_12m_cnt end as cate7006_001_6mv12m
,case when cate7015_005_6m_cnt=-999 or cate7015_005_12m_cnt=-999 then -999 when cate7015_005_12m_cnt=0 then -9 else cate7015_005_6m_cnt/cate7015_005_12m_cnt end as cate7015_005_6mv12m
,case when cate7016_001_6m_cnt=-999 or cate7016_001_12m_cnt=-999 then -999 when cate7016_001_12m_cnt=0 then -9 else cate7016_001_6m_cnt/cate7016_001_12m_cnt end as cate7016_001_6mv12m
,case when cate7016_002_6m_cnt=-999 or cate7016_002_12m_cnt=-999 then -999 when cate7016_002_12m_cnt=0 then -9 else cate7016_002_6m_cnt/cate7016_002_12m_cnt end as cate7016_002_6mv12m
,case when cate7019_122_6m_cnt=-999 or cate7019_122_12m_cnt=-999 then -999 when cate7019_122_12m_cnt=0 then -9 else cate7019_122_6m_cnt/cate7019_122_12m_cnt end as cate7019_122_6mv12m
,case when fin_46_6m_cnt=-999 or fin_46_12m_cnt=-999 then -999 when fin_46_12m_cnt=0 then -9 else fin_46_6m_cnt/fin_46_12m_cnt end as fin_46_6mv12m
,case when tgi1_18_0_6m_cnt=-999 or tgi1_18_0_12m_cnt=-999 then -999 when tgi1_18_0_12m_cnt=0 then -9 else tgi1_18_0_6m_cnt/tgi1_18_0_12m_cnt end as tgi1_18_0_6mv12m
,case when tgi1_25_34_1_6m_cnt=-999 or tgi1_25_34_1_12m_cnt=-999 then -999 when tgi1_25_34_1_12m_cnt=0 then -9 else tgi1_25_34_1_6m_cnt/tgi1_25_34_1_12m_cnt end as tgi1_25_34_1_6mv12m
,case when tgi1_55_0_6m_cnt=-999 or tgi1_55_0_12m_cnt=-999 then -999 when tgi1_55_0_12m_cnt=0 then -9 else tgi1_55_0_6m_cnt/tgi1_55_0_12m_cnt end as tgi1_55_0_6mv12m
,case when tgi2_35_44_6_6m_cnt=-999 or tgi2_35_44_6_12m_cnt=-999 then -999 when tgi2_35_44_6_12m_cnt=0 then -9 else tgi2_35_44_6_6m_cnt/tgi2_35_44_6_12m_cnt end as tgi2_35_44_6_6mv12m
,case when tgi2_45_54_0_6m_cnt=-999 or tgi2_45_54_0_12m_cnt=-999 then -999 when tgi2_45_54_0_12m_cnt=0 then -9 else tgi2_45_54_0_6m_cnt/tgi2_45_54_0_12m_cnt end as tgi2_45_54_0_6mv12m
from(
    select device,cate2_12m_cnt
            ,cate7006_001_12m_cnt
            ,cate7015_005_12m_cnt
            ,cate7016_001_12m_cnt
            ,cate7016_002_12m_cnt
            ,cate7019_122_12m_cnt
            ,fin_46_12m_cnt
            ,tgi1_18_0_12m_cnt
            ,tgi1_25_34_1_12m_cnt
            ,tgi1_55_0_12m_cnt
            ,tgi2_35_44_6_12m_cnt
            ,tgi2_45_54_0_12m_cnt
    from $age_new_ratio_features_12m
    where day='$insertday'
)a 
left join(
    select device,cate2_6m_cnt
        ,cate7006_001_6m_cnt
        ,cate7015_005_6m_cnt
        ,cate7016_001_6m_cnt
        ,cate7016_002_6m_cnt
        ,cate7019_122_6m_cnt
        ,fin_46_6m_cnt
        ,tgi1_18_0_6m_cnt
        ,tgi1_25_34_1_6m_cnt
        ,tgi1_55_0_6m_cnt
        ,tgi2_35_44_6_6m_cnt
        ,tgi2_45_54_0_6m_cnt
    from $age_new_ratio_features_6m
    where day='$insertday'
)b 
on a.device = b.device ;
"