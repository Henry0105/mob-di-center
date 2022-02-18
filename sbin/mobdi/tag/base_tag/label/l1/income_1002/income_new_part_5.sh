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
income_new_ratio_features_3m="${tmpdb}.income_new_ratio_features_3m"



HADOOP_USER_NAME=dba hive -e "
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


insert overwrite table $income_new_ratio_features_3m partition(day='$end_month')
select
    device
    ,a19_3m_cnt
    ,b1_3m_cnt
    ,b12_3m_cnt
    ,round(COALESCE(a1_3m_cnt*1.000000/total_3m_cnt,0),6) as a1_3m_ratio
    ,round(COALESCE(a10_3m_cnt*1.000000/total_3m_cnt,0),6) as a10_3m_ratio
    ,round(COALESCE(a13_1_3m_cnt*1.000000/total_3m_cnt,0),6) as a13_1_3m_ratio
    ,round(COALESCE(a13_3m_cnt*1.000000/total_3m_cnt,0),6) as a13_3m_ratio
    ,round(COALESCE(a2_4_3m_cnt*1.000000/total_3m_cnt,0),6) as a2_4_3m_ratio
    ,round(COALESCE(fin_48_3m_cnt*1.000000/total_3m_cnt,0),6) as fin_48_3m_ratio
    ,round(COALESCE(tgi1_1_0_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_1_0_3m_ratio
    ,round(COALESCE(tgi1_1_2_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_1_2_3m_ratio
    ,round(COALESCE(tgi1_1_3_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_1_3_3m_ratio
    ,round(COALESCE(tgi1_2_0_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_2_0_3m_ratio
    ,round(COALESCE(tgi1_2_1_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_2_1_3m_ratio
    ,round(COALESCE(tgi1_3_0_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_3_0_3m_ratio
    ,round(COALESCE(tgi1_3_2_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_3_2_3m_ratio
    ,round(COALESCE(tgi1_5_2_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_5_2_3m_ratio
    ,round(COALESCE(tgi1_5_3_3m_cnt*1.000000/total_3m_cnt,0),6) as tgi1_5_3_3m_ratio
from
(
      select
              device
            ,count( pkg ) as total_3m_cnt
            ,count(case when cate_id='a1' then 1 else null end) as a1_3m_cnt
            ,count(case when cate_id='a10' then 1 else null end) as a10_3m_cnt
            ,count(case when cate_id='a13' then 1 else null end) as a13_3m_cnt
            ,count(case when cate_id='a13_1' then 1 else null end) as a13_1_3m_cnt
            ,count(case when cate_id='a19' then 1 else null end) as a19_3m_cnt
            ,count(case when cate_id='a2_4' then 1 else null end) as a2_4_3m_cnt
            ,count(case when cate_id='b1' then 1 else null end) as b1_3m_cnt
            ,count(case when cate_id='b12' then 1 else null end) as b12_3m_cnt
            ,count(case when cate_id='fin_48' then 1 else null end) as fin_48_3m_cnt
            ,count(case when cate_id='tgi1_1_0' then 1 else null end) as tgi1_1_0_3m_cnt
            ,count(case when cate_id='tgi1_1_2' then 1 else null end) as tgi1_1_2_3m_cnt
            ,count(case when cate_id='tgi1_1_3' then 1 else null end) as tgi1_1_3_3m_cnt
            ,count(case when cate_id='tgi1_2_0' then 1 else null end) as tgi1_2_0_3m_cnt
            ,count(case when cate_id='tgi1_2_1' then 1 else null end) as tgi1_2_1_3m_cnt
            ,count(case when cate_id='tgi1_3_0' then 1 else null end) as tgi1_3_0_3m_cnt
            ,count(case when cate_id='tgi1_3_2' then 1 else null end) as tgi1_3_2_3m_cnt
            ,count(case when cate_id='tgi1_5_2' then 1 else null end) as tgi1_5_2_3m_cnt
            ,count(case when cate_id='tgi1_5_3' then 1 else null end) as tgi1_5_3_3m_cnt
      from
      (
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
      where datediff(date,update_day)<=90 and  datediff(date,flag_day)<=90
      group by device
)t2 ;
"