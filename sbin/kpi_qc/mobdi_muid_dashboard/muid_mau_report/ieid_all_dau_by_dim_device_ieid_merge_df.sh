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
create table if not exists mobdi_muid_dashboard.ieid_all_dau_by_dim_device_ieid_merge_df(
ieid_history bigint comment '总量',
ieid_yearhalf bigint comment'近一年半总量',
ieid_history_dau bigint comment'全量日新增',
ieid_yearhalf_dau bigint comment'近一年半日新增',
ieid_two_year bigint comment'近两年总量',
ieid_two_year_dau bigint comment'近两年日新增'
)comment 'ieid总量和日新增'
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
insert overwrite table mobdi_muid_dashboard.ieid_all_dau_by_dim_device_ieid_merge_df partition(day='$day')
select
ieid_history,
ieid_yearhalf,
ieid_history-ieid_history_yesterday as ieid_history_dau,
ieid_yearhalf-ieid_yearhalf_yesterday as ieid_yearhalf_dau,
ieid_two_year,
ieid_two_year-ieid_two_year_yesterday as ieid_two_year_dau
from
(
select
count(*) as ieid_history,sum(if(ieid_ltm >= $yearhalf_days,1,0)) as ieid_yearhalf,sum(if(ieid_ltm >= $two_year_ago,1,0)) as ieid_two_year
from
(
select ieid, from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') as ieid_ltm
from
(
  select ieid, max(ieid_ltm) as ieid_ltm
  from
  (
    select concat(ieid, '=', ieid_ltm) as ieid_concat
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as m
  lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
  group by ieid
) as n
group by ieid,ieid_ltm
)c
)a
join
(select ieid_history as ieid_history_yesterday,ieid_yearhalf as ieid_yearhalf_yesterday,ieid_two_year as ieid_two_year_yesterday
from mobdi_muid_dashboard.ieid_all_dau_by_dim_device_ieid_merge_df
where day='$lastday')b
;"


