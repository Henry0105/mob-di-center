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
# 获取当前日期前一年
pre_one_year=$(date -d "${insert_day} -1 year" "+%Y%m%d")

source /home/dba/mobdi_center/conf/hive-env.sh

# input
#income_category_mapping=tp_mobdi_model.income_category_mapping
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
#label_device_pkg_install_uninstall_year_info_mf=dm_mobdi_report.label_device_pkg_install_uninstall_year_info_mf

tmpdb=dm_mobdi_tmp
income_new_active_recency_features_tmp="${tmpdb}.income_new_active_recency_features_tmp"
income_new_applist_install_bycate_id_whether="${tmpdb}.income_new_applist_install_bycate_id_whether"
income_new_active_recency_features="${tmpdb}.income_new_active_recency_features"



HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;


insert overwrite table $income_new_active_recency_features_tmp partition (day='$end_month')
select 
    device
    ,datediff(date_dt,tgi1_1_0_active_max_day) as tgi1_1_0_active_max_day_diff
    ,datediff(date_dt,a12_active_max_day) as a12_active_max_day_diff
    ,datediff(date_dt,tgi1_4_2_active_max_day) as tgi1_4_2_active_max_day_diff
    ,datediff(date_dt,a19_active_max_day) as a19_active_max_day_diff
    ,datediff(date_dt,tgi1_5_3_active_max_day) as tgi1_5_3_active_max_day_diff
    ,datediff(date_dt,tgi1_3_2_active_max_day) as tgi1_3_2_active_max_day_diff
    ,datediff(date_dt,tgi1_2_0_active_max_day) as tgi1_2_0_active_max_day_diff
    ,datediff(date_dt,a1_active_max_day) as a1_active_max_day_diff
    ,datediff(date_dt,tgi1_4_1_active_max_day) as tgi1_4_1_active_max_day_diff
    ,datediff(date_dt,tgi1_1_3_active_min_day) as tgi1_1_3_active_min_day_diff
    ,datediff(date_dt,tgi1_5_2_active_min_day) as tgi1_5_2_active_min_day_diff
    ,datediff(date_dt,fin_46_active_min_day) as fin_46_active_min_day_diff
from 
(
    select 
        device
        ,max(case when cate_id='tgi1_1_0' then max_day else null end) as tgi1_1_0_active_max_day
        ,max(case when cate_id='a12' then max_day else null end) as a12_active_max_day
        ,max(case when cate_id='tgi1_4_2' then max_day else null end) as tgi1_4_2_active_max_day
        ,max(case when cate_id='a19' then max_day else null end) as a19_active_max_day
        ,max(case when cate_id='tgi1_5_3' then max_day else null end) as tgi1_5_3_active_max_day
        ,max(case when cate_id='tgi1_3_2' then max_day else null end) as tgi1_3_2_active_max_day
        ,max(case when cate_id='tgi1_2_0' then max_day else null end) as tgi1_2_0_active_max_day
        ,max(case when cate_id='a1' then max_day else null end) as a1_active_max_day
        ,max(case when cate_id='tgi1_4_1' then max_day else null end) as tgi1_4_1_active_max_day
        ,min(case when cate_id='tgi1_1_3' then min_day else null end) as tgi1_1_3_active_min_day
        ,min(case when cate_id='tgi1_5_2' then min_day else null end) as tgi1_5_2_active_min_day
        ,min(case when cate_id='fin_46' then min_day else null end) as fin_46_active_min_day
        ,date_dt
    from(
        select device,a.cate_id,date_dt,min(day_dt) as min_day,max(day_dt) as max_day
        from
        (
            select pkg, cate_id
            from $income_category_mapping
            where cate_id in ('a1'
                            ,'a12'
                            ,'a19'
                            ,'fin_46'
                            ,'tgi1_1_0'
                            ,'tgi1_1_3'
                            ,'tgi1_2_0'
                            ,'tgi1_3_2'
                            ,'tgi1_4_1'
                            ,'tgi1_4_2'
                            ,'tgi1_5_2'
                            ,'tgi1_5_3')
            group by pkg, cate_id
        )a 
        join
        (
            select 
              device,pkg,day
              ,from_unixtime(unix_timestamp('$end_month','yyyyMMdd'),'yyyy-MM-dd') as date_dt
              ,from_unixtime(unix_timestamp(day,'yyyyMMdd'),'yyyy-MM-dd') as day_dt
            from $dws_device_active_applist_di
            where day between '$pre_one_year' and '$end_month'
        )b
        on a.pkg=b.pkg
        group by device,a.cate_id,date_dt
    )t1 
    group by device,date_dt
)t2;
"


HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;


insert overwrite table $income_new_applist_install_bycate_id_whether partition (day='$end_month')
select
    device
    ,sum(case when cate_id='a1' then 1 else 0 end )as flag_12m_a1
    ,sum(case when cate_id='a12' then 1 else 0 end )as flag_12m_a12
    ,sum(case when cate_id='a19' then 1 else 0 end )as flag_12m_a19
    ,sum(case when cate_id='fin_46' then 1 else 0 end )as flag_12m_fin_46
    ,sum(case when cate_id='tgi1_1_0' then 1 else 0 end )as flag_12m_tgi1_1_0
    ,sum(case when cate_id='tgi1_1_3' then 1 else 0 end )as flag_12m_tgi1_1_3
    ,sum(case when cate_id='tgi1_2_0' then 1 else 0 end )as flag_12m_tgi1_2_0
    ,sum(case when cate_id='tgi1_3_2' then 1 else 0 end )as flag_12m_tgi1_3_2
    ,sum(case when cate_id='tgi1_4_1' then 1 else 0 end )as flag_12m_tgi1_4_1
    ,sum(case when cate_id='tgi1_4_2' then 1 else 0 end )as flag_12m_tgi1_4_2
    ,sum(case when cate_id='tgi1_5_2' then 1 else 0 end )as flag_12m_tgi1_5_2
    ,sum(case when cate_id='tgi1_5_3' then 1 else 0 end )as flag_12m_tgi1_5_3
from
(
    select device,b1.pkg,cate_id,refine_final_flag,day,update_day
    from(
        select pkg, cate_id
        from $income_category_mapping
        where cate_id in ('a1'
                        ,'a12'
                        ,'a19'
                        ,'fin_46'
                        ,'tgi1_1_0'
                        ,'tgi1_1_3'
                        ,'tgi1_2_0'
                        ,'tgi1_3_2'
                        ,'tgi1_4_1'
                        ,'tgi1_4_2'
                        ,'tgi1_5_2'
                        ,'tgi1_5_3')
        group by pkg, cate_id
    )b1
    inner join(
         select device,pkg,refine_final_flag,day,update_day
        from $label_device_pkg_install_uninstall_year_info_mf
        where day='$end_month' and update_day between '$start_month' and '$end_month'
        group by device,pkg,refine_final_flag,update_day,day
    )b2
    on b1.pkg = b2.pkg
)b
group by device
"


HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;


insert overwrite table $income_new_active_recency_features partition (day='$end_month')
select a.device
,case when tgi1_1_0_active_max_day_diff >0 then tgi1_1_0_active_max_day_diff   when flag_12m_tgi1_1_0=0 then -99 when tgi1_1_0_active_max_day_diff  is null then -95 else tgi1_1_0_active_max_day_diff  end as tgi1_1_0_active_max_day_diff
,case when a12_active_max_day_diff >0 then a12_active_max_day_diff   when flag_12m_a12=0 then -99 when a12_active_max_day_diff  is null then -95 else a12_active_max_day_diff  end as a12_active_max_day_diff
,case when tgi1_4_2_active_max_day_diff >0 then tgi1_4_2_active_max_day_diff   when flag_12m_tgi1_4_2=0 then -99 when tgi1_4_2_active_max_day_diff  is null then -95 else tgi1_4_2_active_max_day_diff  end as tgi1_4_2_active_max_day_diff
,case when a19_active_max_day_diff >0 then a19_active_max_day_diff   when flag_12m_a19=0 then -99 when a19_active_max_day_diff  is null then -95 else a19_active_max_day_diff  end as a19_active_max_day_diff
,case when tgi1_5_3_active_max_day_diff >0 then tgi1_5_3_active_max_day_diff   when flag_12m_tgi1_5_3=0 then -99 when tgi1_5_3_active_max_day_diff  is null then -95 else tgi1_5_3_active_max_day_diff  end as tgi1_5_3_active_max_day_diff
,case when tgi1_3_2_active_max_day_diff >0 then tgi1_3_2_active_max_day_diff   when flag_12m_tgi1_3_2=0 then -99 when tgi1_3_2_active_max_day_diff  is null then -95 else tgi1_3_2_active_max_day_diff  end as tgi1_3_2_active_max_day_diff
,case when tgi1_2_0_active_max_day_diff >0 then tgi1_2_0_active_max_day_diff   when flag_12m_tgi1_2_0=0 then -99 when tgi1_2_0_active_max_day_diff  is null then -95 else tgi1_2_0_active_max_day_diff  end as tgi1_2_0_active_max_day_diff
,case when a1_active_max_day_diff >0 then a1_active_max_day_diff   when flag_12m_a1=0 then -99 when a1_active_max_day_diff  is null then -95 else a1_active_max_day_diff  end as a1_active_max_day_diff
,case when tgi1_4_1_active_max_day_diff >0 then tgi1_4_1_active_max_day_diff   when flag_12m_tgi1_4_1=0 then -99 when tgi1_4_1_active_max_day_diff  is null then -95 else tgi1_4_1_active_max_day_diff  end as tgi1_4_1_active_max_day_diff
,case when tgi1_1_3_active_min_day_diff >0 then tgi1_1_3_active_min_day_diff   when flag_12m_tgi1_1_3=0 then -99 when tgi1_1_3_active_min_day_diff  is null then -95 else tgi1_1_3_active_min_day_diff  end as tgi1_1_3_active_min_day_diff
,case when tgi1_5_2_active_min_day_diff >0 then tgi1_5_2_active_min_day_diff   when flag_12m_tgi1_5_2=0 then -99 when tgi1_5_2_active_min_day_diff  is null then -95 else tgi1_5_2_active_min_day_diff  end as tgi1_5_2_active_min_day_diff
,case when fin_46_active_min_day_diff >0 then fin_46_active_min_day_diff   when flag_12m_fin_46=0 then -99 when fin_46_active_min_day_diff  is null then -95 else fin_46_active_min_day_diff  end as fin_46_active_min_day_diff
from(
    select *
    from $applist_install_bycate_id_whether
    where day='$end_month'
)a
left join(
    select *
    from $income_new_active_recency_features_tmp
    where day='$end_month'
)b
on a.device = b.device;
"

