#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

insert_day=$1

# 获取当前日期所在月的第一天
start_month=$(date -d "${insert_day}  " +%Y%m01)
# 获取当前日期所在月的最后一天
end_month=$insert_day

source /home/dba/mobdi_center/conf/hive-env.sh

# input
#income_category_mapping=tp_mobdi_model.income_category_mapping
#label_device_pkg_install_uninstall_year_info_mf=dm_mobdi_report.label_device_pkg_install_uninstall_year_info_mf

tmpdb=dm_mobdi_tmp
income_new_Ins_recency_features="${tmpdb}.income_new_Ins_recency_features"



HADOOP_USER_NAME=dba hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;


-- step2: 计算在装最近/最早一次距今时长
insert overwrite table $income_new_Ins_recency_features partition (day='$end_month')
select
     device
    ,datediff(date_dt,tgi1_1_0_install_first_max_diff) as tgi1_1_0_install_first_max_diff
    ,datediff(date_dt,tgi1_1_3_install_first_max_diff) as tgi1_1_3_install_first_max_diff
    ,datediff(date_dt,fin_46_install_first_max_diff) as fin_46_install_first_max_diff
    ,datediff(date_dt,a19_install_first_max_diff) as a19_install_first_max_diff
    ,datediff(date_dt,a1_install_first_max_diff) as a1_install_first_max_diff
    ,datediff(date_dt,a10_2_install_first_max_diff) as a10_2_install_first_max_diff
    ,datediff(date_dt,tgi1_5_2_install_first_max_diff) as tgi1_5_2_install_first_max_diff
    ,datediff(date_dt,a12_install_first_max_diff) as a12_install_first_max_diff
from
    (select
        device

        ,max(case when cate_id='tgi1_1_0' then update_day else null end) as tgi1_1_0_install_first_max_diff
        ,max(case when cate_id='tgi1_1_3' then update_day else null end) as tgi1_1_3_install_first_max_diff
        ,max(case when cate_id='fin_46' then update_day else null end) as fin_46_install_first_max_diff
        ,max(case when cate_id='a19' then update_day else null end) as a19_install_first_max_diff
        ,max(case when cate_id='a1' then update_day else null end) as a1_install_first_max_diff
        ,max(case when cate_id='a10_2' then update_day else null end) as a10_2_install_first_max_diff
        ,max(case when cate_id='tgi1_5_2' then update_day else null end) as tgi1_5_2_install_first_max_diff
        ,max(case when cate_id='a12' then update_day else null end) as a12_install_first_max_diff
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
                        from $income_category_mapping
                        where cate_id in ('tgi1_1_0','tgi1_1_3','fin_46','a19','a1','a10_2','tgi1_5_2','a12')
                        group by pkg, cate_id
                    )a
                    join
                    (
                        select device,pkg,refine_final_flag,update_day,flag_day,day
                        from $label_device_pkg_install_uninstall_year_info_mf
                        where day='$end_month' and update_day between '$start_month' and '$end_month'
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