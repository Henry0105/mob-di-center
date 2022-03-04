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
create table if not exists mobdi_muid_dashboard.isid_all_dau_by_dim_device_isid_merge_df(
isid_history bigint comment '总量',
isid_yearhalf bigint comment'近一年半总量',
isid_history_dau bigint comment'全量日新增',
isid_yearhalf_dau bigint comment'近一年半日新增'
isid_two_year bigint comment'近两年总量',
isid_two_year_dau bigint comment'近两年日新增'
)comment 'isid总量和日新增'
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
insert overwrite table mobdi_muid_dashboard.isid_all_dau_by_dim_device_isid_merge_df partition(day='$day')
select
isid_history,
isid_yearhalf,
isid_history-isid_history_yesterday as isid_history_dau,
isid_yearhalf-isid_yearhalf_yesterday as isid_yearhalf_dau,
isid_two_year,
isid_two_year-isid_two_year_yesterday as isid_two_year_dau
from
(
select
count(*) as isid_history,sum(if(isid_ltm >= $yearhalf_days,1,0)) as isid_yearhalf,sum(if(isid_ltm >= $two_year_ago,1,0)) as isid_two_year
from
(
select isid, from_unixtime(cast(isid_ltm as bigint), 'yyyyMMdd') as isid_ltm
from
(
  select isid, max(isid_ltm) as isid_ltm
  from
  (
    select concat(isid, '=', isid_ltm) as isid_concat
    from dim_mobdi_mapping.dim_device_isid_merge_df
    where day = '$day' and length(isid) > 0
  ) as m
  lateral view explode_tags(isid_concat) mytable as isid, isid_ltm
  group by isid
) as n
group by isid,isid_ltm
)c
)a
join
(select isid_history as isid_history_yesterday,isid_yearhalf as isid_yearhalf_yesterday,isid_two_year as isid_two_year_yesterday
from mobdi_muid_dashboard.isid_all_dau_by_dim_device_isid_merge_df
where day='$lastday')b
on 1=1
;"



