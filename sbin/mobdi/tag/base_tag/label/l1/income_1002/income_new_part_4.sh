#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

insert_day=$1

# 获取当前日期的下个月第一天
nextmonth=$(date -d "${insert_day} +1 month" +%Y%m01)
# 获取当前日期所在月的第一天
start_month=$(date -d "${insert_day}  "+%Y%m01)
# 获取当前日期所在月的最后一天
end_month=$(date -d "$nextmonth last day" +%Y%m%d)

source /home/dba/mobdi_center/conf/hive-env.sh

# input
#income_category_mapping=tp_mobdi_model.income_category_mapping
#label_device_pkg_install_uninstall_year_info_mf=dm_mobdi_report.label_device_pkg_install_uninstall_year_info_mf

tmpdb=dm_mobdi_tmp
income_new_ratio_features_6m="${tmpdb}.income_new_ratio_features_6m"



HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;


insert overwrite table $income_new_ratio_features_6m partition(day='$end_month')
select device
      ,a12_6m_cnt
      ,a19_6m_cnt
      ,a2_6m_cnt
      ,a3_6m_cnt
      ,a8_1_6m_cnt
      ,a8_6m_cnt
      ,b1_6m_cnt
      ,b12_6m_cnt
      ,b8_1_6m_cnt
      ,fin_48_6m_cnt
      ,total_6m_cnt
      ,a1_6m_ratio
      ,a10_6m_ratio
      ,a13_1_6m_ratio
      ,a2_4_6m_ratio
      ,a8_6m_ratio
      ,b3_6m_ratio
      ,fin_48_6m_ratio
      ,tgi1_1_0_6m_ratio
      ,tgi1_1_2_6m_ratio
      ,tgi1_1_3_6m_ratio
      ,tgi1_3_0_6m_ratio
      ,tgi1_3_1_6m_ratio
      ,tgi1_3_2_6m_ratio
      ,tgi1_4_1_6m_ratio
      ,tgi1_4_2_6m_ratio
      ,tgi1_5_2_6m_ratio
      ,tgi1_5_3_6m_ratio
from(
select
    *
     ,round(COALESCE(a1_6m_cnt*1.000000/total_6m_cnt,0),6) as a1_6m_ratio
     ,round(COALESCE(a10_6m_cnt*1.000000/total_6m_cnt,0),6) as a10_6m_ratio
     ,round(COALESCE(a13_1_6m_cnt*1.000000/total_6m_cnt,0),6) as a13_1_6m_ratio
     ,round(COALESCE(a2_4_6m_cnt*1.000000/total_6m_cnt,0),6) as a2_4_6m_ratio
     ,round(COALESCE(a8_6m_cnt*1.000000/total_6m_cnt,0),6) as a8_6m_ratio
     ,round(COALESCE(b3_6m_cnt*1.000000/total_6m_cnt,0),6) as b3_6m_ratio
     ,round(COALESCE(fin_48_6m_cnt*1.000000/total_6m_cnt,0),6) as fin_48_6m_ratio
     ,round(COALESCE(tgi1_1_0_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_1_0_6m_ratio
     ,round(COALESCE(tgi1_1_2_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_1_2_6m_ratio
     ,round(COALESCE(tgi1_1_3_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_1_3_6m_ratio
     ,round(COALESCE(tgi1_3_0_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_3_0_6m_ratio
     ,round(COALESCE(tgi1_3_1_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_3_1_6m_ratio
     ,round(COALESCE(tgi1_3_2_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_3_2_6m_ratio
     ,round(COALESCE(tgi1_4_1_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_4_1_6m_ratio
     ,round(COALESCE(tgi1_4_2_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_4_2_6m_ratio
     ,round(COALESCE(tgi1_5_2_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_5_2_6m_ratio
     ,round(COALESCE(tgi1_5_3_6m_cnt*1.000000/total_6m_cnt,0),6) as tgi1_5_3_6m_ratio
from(
        select device
        ,count( pkg  ) as total_6m_cnt
        ,count(case when cate_id='a1' then pkg else null end) as a1_6m_cnt
        ,count(case when cate_id='a10' then pkg else null end) as a10_6m_cnt
        ,count(case when cate_id='a12' then pkg else null end) as a12_6m_cnt
        ,count(case when cate_id='a13_1' then pkg else null end) as a13_1_6m_cnt
        ,count(case when cate_id='a19' then pkg else null end) as a19_6m_cnt
        ,count(case when cate_id='a2' then pkg else null end) as a2_6m_cnt
        ,count(case when cate_id='a2_4' then pkg else null end) as a2_4_6m_cnt
        ,count(case when cate_id='a3' then pkg else null end) as a3_6m_cnt
        ,count(case when cate_id='a8' then pkg else null end) as a8_6m_cnt
        ,count(case when cate_id='a8_1' then pkg else null end) as a8_1_6m_cnt
        ,count(case when cate_id='b1' then pkg else null end) as b1_6m_cnt
        ,count(case when cate_id='b12' then pkg else null end) as b12_6m_cnt
        ,count(case when cate_id='b3' then pkg else null end) as b3_6m_cnt
        ,count(case when cate_id='b8_1' then pkg else null end) as b8_1_6m_cnt
        ,count(case when cate_id='fin_48' then pkg else null end) as fin_48_6m_cnt
        ,count(case when cate_id='tgi1_1_0' then pkg else null end) as tgi1_1_0_6m_cnt
        ,count(case when cate_id='tgi1_1_2' then pkg else null end) as tgi1_1_2_6m_cnt
        ,count(case when cate_id='tgi1_1_3' then pkg else null end) as tgi1_1_3_6m_cnt
        ,count(case when cate_id='tgi1_3_0' then pkg else null end) as tgi1_3_0_6m_cnt
        ,count(case when cate_id='tgi1_3_1' then pkg else null end) as tgi1_3_1_6m_cnt
        ,count(case when cate_id='tgi1_3_2' then pkg else null end) as tgi1_3_2_6m_cnt
        ,count(case when cate_id='tgi1_4_1' then pkg else null end) as tgi1_4_1_6m_cnt
        ,count(case when cate_id='tgi1_4_2' then pkg else null end) as tgi1_4_2_6m_cnt
        ,count(case when cate_id='tgi1_5_2' then pkg else null end) as tgi1_5_2_6m_cnt
        ,count(case when cate_id='tgi1_5_3' then pkg else null end) as tgi1_5_3_6m_cnt
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
                from $income_category_mapping
                group by pkg, cate_id
            )a 
            join
            (
                select device,pkg,refine_final_flag,first_day,flag_day,update_day,day
                from $label_device_pkg_install_uninstall_year_info_mf
                where day='$end_month' and update_day between '$start_month' and '$end_month'
            )b
            on a.pkg=b.pkg
        )t1
         where datediff(date,update_day)<=180 and  datediff(date,flag_day)<=180
        group by device
    )t2 
)t3;
"