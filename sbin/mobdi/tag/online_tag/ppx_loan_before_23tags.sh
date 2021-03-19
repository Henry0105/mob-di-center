#!/bin/bash

set -x -e

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#input
#dwd_log_device_unstall_app_info_sec_di=dm_mobdi_master.dwd_log_device_unstall_app_info_sec_di
#dwd_log_device_install_app_incr_info_sec_di=dm_mobdi_master.dwd_log_device_install_app_incr_info_sec_di

#out
#timewindow_online_profile_v3=dm_mobdi_report.timewindow_online_profile_v3
#rp_fintech_pre_loan_label=dm_mobdi_report.rp_fintech_pre_loan_label

#金融线贷前标签
if [ $# -eq 2 ] ; then
  from_date=$1
  end_date=$2
elif [ $# -eq 1 ] ; then
  from_date=$1
  end_date=${from_date}
  echo "PreLoanLabel|params num [1]: endDate will be same with fromDate[${from_date}]"
elif [ $# -eq 0 ] ; then
  from_date=`date -d "1 day ago" +%Y%m%d`
  end_date=${from_date}
  echo "PreLoanLabel|params num [0]: endDate, fromDate will be same with date[1 day ago/${from_date}]"
else
  echo "PreLoanLabel|params num mismatch: should be [<=2]"
  exit 1
fi

rank_date=${from_date}
echo "insert ${rp_fintech_pre_loan_label} parition day=${rank_date}"

while((${rank_date}<=${end_date}))
do
  days_60_before=$(date -d "60 day ago ${rank_date}" +%Y%m%d)
  days_30_before=$(date -d "30 day ago ${rank_date}" +%Y%m%d)
  days_21_before=$(date -d "21 day ago ${rank_date}" +%Y%m%d)
  days_14_before=$(date -d "14 day ago ${rank_date}" +%Y%m%d)
  days_7_before=$(date -d "7 day ago ${rank_date}" +%Y%m%d)
  days_1_before=$(date -d "1 day ago ${rank_date}" +%Y%m%d)

  hive -e "
      SET mapred.max.split.size=256000000;
      SET mapred.min.split.size.per.node=100000000;
      SET mapred.min.split.size.per.rack=100000000;
      SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
      SET hive.merge.smallfiles.avgsize=256000000;
      SET hive.merge.mapfiles = true;
      SET hive.merge.mapredfiles = true;
      SET hive.merge.size.per.task = 256000000;
      SET hive.hadoop.supports.splittable.combineinputformat=true;
      SET hive.exec.parallel=true;

      INSERT OVERWRITE TABLE ${rp_fintech_pre_loan_label} PARTITION (day='${rank_date}')
      select
        t1.device,
        t1.pre_wthin_5_in_un_cnt_unique_app,
        t1.pre_wthin_5_in_un_cnt_unique_day,
        t1.pre_wthin_6_in_un_cnt_unique_app,
        t1.pre_wthin_6_in_un_cnt_unique_day,
        t1.pre_wthin_4_un_cnt_unique_app,
        t2.pre_wthin_3_un_cnt_times,
        t2.pre_wthin_4_un_cnt_times,
        t1.pre_wthin_5_un_cnt_unique_app,
        t1.pre_wthin_3_un_cnt_unique_app,
        t1.pre_wthin_4_un_cnt_unique_day,
        t2.pre_wthin_5_un_cnt_times,
        t1.pre_wthin_6_un_cnt_unique_app,
        t2.pre_wthin_6_un_cnt_times,
        t1.pre_wthin_3_un_cnt_unique_day,
        t2.pre_wthin_2_un_cnt_times,
        t1.pre_wthin_2_un_cnt_unique_app,
        t1.pre_wthin_2_un_cnt_unique_day,
        t1.pre_wthin_1_un_cnt_unique_day
      from
      (select
          device
         ,COUNT(DISTINCT (CASE WHEN day>=${days_30_before} THEN pkg end)) as pre_wthin_5_in_un_cnt_unique_app
         ,COUNT(DISTINCT (CASE WHEN day>=${days_30_before} THEN day end)) as pre_wthin_5_in_un_cnt_unique_day
         ,COUNT(DISTINCT pkg) as pre_wthin_6_in_un_cnt_unique_app
         ,COUNT(DISTINCT day) as pre_wthin_6_in_un_cnt_unique_day
         ,COUNT(DISTINCT (CASE WHEN day>=${days_21_before} AND action_type='unstall' then pkg end)) as pre_wthin_4_un_cnt_unique_app
         ,COUNT(DISTINCT (CASE WHEN day>=${days_30_before} AND action_type='unstall' then pkg end)) as pre_wthin_5_un_cnt_unique_app
         ,COUNT(DISTINCT (CASE WHEN day>=${days_14_before} AND action_type='unstall' then pkg end)) as pre_wthin_3_un_cnt_unique_app
         ,COUNT(DISTINCT (CASE WHEN day>=${days_21_before} AND action_type='unstall' then day end)) as pre_wthin_4_un_cnt_unique_day
         ,COUNT(DISTINCT (CASE WHEN day>=${days_60_before} AND action_type='unstall' then pkg end)) as pre_wthin_6_un_cnt_unique_app
         ,COUNT(DISTINCT (CASE WHEN day>=${days_14_before} AND action_type='unstall' then day end)) as pre_wthin_3_un_cnt_unique_day
         ,COUNT(DISTINCT (CASE WHEN day>=${days_7_before} AND action_type='unstall' then pkg end)) as pre_wthin_2_un_cnt_unique_app
         ,COUNT(DISTINCT (CASE WHEN day>=${days_7_before} AND action_type='unstall' then day end)) as pre_wthin_2_un_cnt_unique_day
         ,COUNT(DISTINCT (CASE WHEN day>=${days_1_before} AND action_type='unstall' then day end)) as pre_wthin_1_un_cnt_unique_day
       from
       (select
         device,pkg,day,action_type
        FROM
         (
           SELECT deviceid as device,pkg,day,'unstall' as action_type
           FROM $dwd_log_device_unstall_app_info_sec_di
           WHERE day>='${days_60_before}' AND day<='${rank_date}' and pkg<>''
         UNION ALL
           SELECT device,pkg,day,'install' as action_type
           FROM $dwd_log_device_install_app_incr_info_sec_di
           WHERE day>='${days_60_before}' AND day<='${rank_date}' and pkg<>''
         ) a
        GROUP BY device,pkg,day,action_type
       ) t
       group by device
      ) t1
      join
      (select
         device
        ,COUNT((CASE WHEN day>=${days_14_before} AND action_type='unstall' then 1 end)) as pre_wthin_3_un_cnt_times
        ,COUNT((CASE WHEN day>=${days_21_before} AND action_type='unstall' then 1 end)) as pre_wthin_4_un_cnt_times
        ,COUNT((CASE WHEN day>=${days_30_before} AND action_type='unstall' then 1 end)) as pre_wthin_5_un_cnt_times
        ,COUNT((CASE WHEN day>=${days_60_before} AND action_type='unstall' then 1 end)) as pre_wthin_6_un_cnt_times
        ,COUNT((CASE WHEN day>=${days_7_before} AND action_type='unstall' then 1 end)) as pre_wthin_2_un_cnt_times
       from
       (
           SELECT deviceid as device,pkg,day,'unstall' as action_type
           FROM $dwd_log_device_unstall_app_info_sec_di
           WHERE day>='${days_60_before}' AND day<='${rank_date}' and pkg<>''
         UNION ALL
           SELECT device,pkg,day,'install' as action_type
           FROM $dwd_log_device_install_app_incr_info_sec_di
           WHERE day>='${days_60_before}' AND day<='${rank_date}' and pkg<>''
       ) a
       group by device
      ) t2
      on t1.device=t2.device
  "
 
echo 'cool'

#插入到v3表中
hive -v -e "

SET mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx8192M';
set mapreduce.child.map.java.opts='-Xmx8192M';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts=-Xmx8192m;
set mapred.reduce.tasks=4000;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table $timewindow_online_profile_v3 partition(day=${rank_date},feature)
select device,cnt,feature from
(
select device,pre_wthin_5_in_un_cnt_unique_app as cnt,'5417_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_5_in_un_cnt_unique_day as cnt,'5418_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_6_in_un_cnt_unique_app as cnt,'5419_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_6_in_un_cnt_unique_day as cnt,'5420_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_4_un_cnt_unique_app as cnt,'5421_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_3_un_cnt_times as cnt,'5422_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_4_un_cnt_times as cnt,'5423_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_5_un_cnt_unique_app as cnt,'5424_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_3_un_cnt_unique_app as cnt,'5425_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_4_un_cnt_unique_day as cnt,'5426_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_5_un_cnt_times as cnt,'5427_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_6_un_cnt_unique_app as cnt,'5428_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_6_un_cnt_times as cnt,'5429_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_3_un_cnt_unique_day as cnt,'5430_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_2_un_cnt_times as cnt,'5431_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_2_un_cnt_unique_app as cnt,'5432_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_2_un_cnt_unique_day as cnt,'5433_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
union all
select device,pre_wthin_1_un_cnt_unique_day as cnt,'5434_1000' as feature
from $rp_fintech_pre_loan_label where day=${rank_date}
)a where cnt !=0
 "


rank_date=`date -d "$rank_date +1 day" +%Y%m%d`
wait
done

