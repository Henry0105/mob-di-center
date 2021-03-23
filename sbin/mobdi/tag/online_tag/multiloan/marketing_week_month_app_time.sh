# !/bin/bash

set -e -x

: '
@owner: yeyy
@describe: (1)统计营销线，6种金融类pkg,在30,60,90天内跨周、跨月数
           (2)统计营销线，6种金融类pkg,在30,60,90天内，节假日和工作日的pkg个数
           运行周期 7天一次 每周一运行
'

if [ $# -lt 2 ]; then
 echo "ERROR: wrong number of parameters"
 echo "USAGE: <date>"
 exit 1
fi

day=$1
timewindow=$2

partition=${day}_${timewindow}

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

### 源表
tmp_pre_process_label=dw_mobdi_tmp.tmp_pre_process_label

### 中间映射表
#vacation_flag=dim_sdk_mapping.vacation_flag

###  结果表
#timewindow_online_profile_v2=dm_mobdi_report.timewindow_online_profile_v2

## stall_flag 1-> 安装 2->卸载
## vacation_flag 1->法定假期、周六，周日
## 安装 90天跨自然月和跨周数（去重）
## flag="10" => "跨周安装次数统计"
## flag="11" => "跨周卸载次数统计"
## flag="12" => "跨月安装次数统计"
## flag="13" => "跨月卸载次数统计"
## flag="14" => "工作日安装app次数统计"
## flag="15" => "工作日卸载app次数统计"
## flag="16" => "节假日安装app次数统计"
## flag="17" => "节假日卸载app次数统计"

flag=10

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       week_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(distinct(week)) as week_cnt
  from
  (
    select device,cate_id,pkg,week
    from $tmp_pre_process_label
    where day='$partition'
    and refine_final_flag='1'
  ) t1
  group by device,cate_id
)tt;"

flag=11

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       week_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(distinct(week)) as week_cnt
  from
  (
    select device,cate_id,pkg,week
    from $tmp_pre_process_label
    where day='$partition'
    and refine_final_flag='-1'
  ) t1
  group by device,cate_id
)tt;"

flag=12

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       month_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(distinct(month)) as month_cnt
  from
  (
    select device,cate_id,pkg,month
    from $tmp_pre_process_label
    where day='$partition'
    and refine_final_flag = '1'
  ) t1
  group by device,cate_id
)tt;"

flag=13

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       month_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(distinct(month)) as month_cnt
  from
  (
    select device,cate_id,pkg,month
    from $tmp_pre_process_label
    where day='$partition'
    and refine_final_flag='-1'
  ) t1
  group by device,cate_id
)tt;"

flag=14

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       pkg_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(1) as pkg_cnt
  from
  (
    select device,cate_id,pkg,current_day
    from
    (
      select device,
             pkg,
             cate_id,
             refine_final_flag,
             month,week,
             current_day
      from
      (
        select device,
               pkg,cate_id,
               refine_final_flag,
               month,week,
               current_day
        from $tmp_pre_process_label
        where day='$partition'
      ) t1
      left join $vacation_flag t2
      on t1.current_day = t2.day
      where t2.day is null
    ) t1
    where pmod(datediff(to_date(from_unixtime(unix_timestamp(current_day,'yyyyMMdd'),'yyyy-MM-dd')), '2012-01-01'), 7) in(1,2,3,4,5)
    and refine_final_flag=1
  ) tt
  group by device,cate_id
)tt;"

flag=15

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,concat_ws('_',cate_id,'$flag','$timewindow') as feature,pkg_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(1) as pkg_cnt
  from
  (
    select device,
           cate_id,
           pkg,
           current_day
    from
    (
      select device,
             pkg,
             cate_id,
             refine_final_flag,
             month,
             week,
             current_day
      from
      (
        select device,
               pkg,
               cate_id,
               refine_final_flag,
               month,
               week,
               current_day
        from $tmp_pre_process_label
        where day='$partition'
      )t1
      left join $vacation_flag t2
      on t1.current_day=t2.day
      where t2.day is null
    ) t1
    where pmod(datediff(to_date(from_unixtime(unix_timestamp(current_day,'yyyyMMdd'),'yyyy-MM-dd')), '2012-01-01'), 7) in(1,2,3,4,5)
    and refine_final_flag=-1
  ) tt
  group by device,cate_id
)tt;"

flag=16

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       pkg_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(1) as pkg_cnt,
         '1' as stall_flag,
         '1' as vacation_flag
  from
  (
    select device,
           cate_id,
           pkg,
           current_day
    from $tmp_pre_process_label t1
    inner join $vacation_flag t2
    on t1.current_day=t2.day
    and refine_final_flag=1
    and t1.day='$partition'
    and pmod(datediff(to_date(from_unixtime(unix_timestamp(current_day,'yyyyMMdd'),'yyyy-MM-dd')), '2012-01-01'), 7) in(1,2,3,4,5)

    union all

    select device,
           cate_id,
           pkg,
           current_day
    from $tmp_pre_process_label t1
    where pmod(datediff(to_date(from_unixtime(unix_timestamp(current_day,'yyyyMMdd'),'yyyy-MM-dd')), '2012-01-01'), 7) in(0,6)
    and refine_final_flag=1
    and day='$partition'
  ) tt
  group by device,cate_id
)tt;"

flag=17

hive -v -e"
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

insert overwrite table $timewindow_online_profile_v2 partition (day='$day',timewindow='$timewindow',flag=$flag)
select device,
       concat_ws('_',cate_id,'$flag','$timewindow') as feature,
       pkg_cnt as cnt
from
(
  select device,
         cast (cate_id as string) as cate_id,
         count(1) as pkg_cnt,
         '1' as stall_flag,
         '1' as vacation_flag
  from
  (
    select device,
           cate_id,
           pkg,
           current_day
    from $tmp_pre_process_label t1
    inner join $vacation_flag t2
    on t1.current_day=t2.day
    and refine_final_flag=-1
    and t1.day='$partition'
    and pmod(datediff(to_date(from_unixtime(unix_timestamp(current_day,'yyyyMMdd'),'yyyy-MM-dd')), '2012-01-01'), 7) in(1,2,3,4,5)

    union all

    select device,
           cate_id,
           pkg,
           current_day
    from $tmp_pre_process_label t1
    where pmod(datediff(to_date(from_unixtime(unix_timestamp(current_day,'yyyyMMdd'),'yyyy-MM-dd')), '2012-01-01'), 7) in(0,6)
    and refine_final_flag=-1
    and day='$partition'
  ) tt
  group by device,cate_id
)tt;"

