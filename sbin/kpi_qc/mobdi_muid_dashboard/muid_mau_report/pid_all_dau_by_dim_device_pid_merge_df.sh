#!/bin/bash

set -e -x

: '
@owner:baron
@describe:muid ieid总量和日新增
@projectName:mobdi_muid_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1
lastday=`date -d "$day -1 days" +%Y%m%d`
yearhalf_days=`date -d "$day -540 days" +%Y%m%d`
two_year_ago=`date -d "$day -2 years" +%Y%m%d`
:<<!
create table if not exists mobdi_muid_dashboard.pid_all_dau_by_dim_device_pid_merge_df(
pid_history bigint comment '总量',
pid_yearhalf bigint comment'近一年半总量',
pid_history_dau bigint comment'全量日新增',
pid_yearhalf_dau bigint comment'近一年半日新增',
pid_two_year bigint comment'近两年总量',
pid_two_year_dau bigint comment'近两年日新增'
)comment 'pid总量和日新增'
partitioned by (day string comment'日期')
stored as orc;
!

HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.groupby.skewindata=true;
set hive.map.aggr=true;
set mapred.task.timeout=1800000;
insert overwrite table mobdi_muid_dashboard.pid_all_dau_by_dim_device_pid_merge_df partition(day='$day')
select
pid_history,
pid_yearhalf,
pid_history-pid_history_yesterday as pid_history_dau,
pid_yearhalf-pid_yearhalf_yesterday as pid_yearhalf_dau,
pid_two_year,
pid_two_year-pid_two_year_yesterday as pid_two_year_dau
from
( 
select
count(*) as pid_history,sum(if(pid_ltm >= $yearhalf_days,1,0)) as pid_yearhalf,sum(if(pid_ltm >= $two_year_ago,1,0)) as pid_two_year
from
(
select pid, from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') as pid_ltm
from
(
  select pid, max(pid_ltm) as pid_ltm
  from
  (
    select concat(pid, '=', pid_ltm) as pid_concat
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as m
  lateral view explode_tags(pid_concat) mytable as pid, pid_ltm
  group by pid
) as n
group by pid,pid_ltm
)c
)a
join
(select pid_history as pid_history_yesterday,pid_yearhalf as pid_yearhalf_yesterday,pid_two_year as pid_two_year_yesterday
from mobdi_muid_dashboard.pid_all_dau_by_dim_device_pid_merge_df
where day='$lastday')b
;"



