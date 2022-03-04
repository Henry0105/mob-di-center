#!/bin/bash

set -e -x

: '
@owner:baron
@describe:muid oiid总量和日新增
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
create table if not exists mobdi_muid_dashboard.oiid_all_dau_by_dim_device_oiid_merge_df(
oiid_history bigint comment '总量',
oiid_yearhalf bigint comment'近一年半总量',
oiid_history_dau bigint comment'全量日新增',
oiid_yearhalf_dau bigint comment'近一年半日新增',
oiid_two_year bigint comment'近两年总量',
oiid_two_year_dau bigint comment'近两年日新增'
)comment 'oiid总量和日新增'
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
insert overwrite table mobdi_muid_dashboard.oiid_all_dau_by_dim_device_oiid_merge_df partition(day='$day')
select
oiid_history,
oiid_yearhalf,
oiid_history-oiid_history_yesterday as oiid_history_dau,
oiid_yearhalf-oiid_yearhalf_yesterday as oiid_yearhalf_dau,
oiid_two_year,
oiid_two_year-oiid_two_year_yesterday as oiid_two_year_dau
from
(
select
count(*) as oiid_history,sum(if(oiid_ltm >= $yearhalf_days,1,0)) as oiid_yearhalf,sum(if(oiid_ltm >= $two_year_ago,1,0)) as oiid_two_year
from
(
select oiid, from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') as oiid_ltm
from
(
  select oiid, max(oiid_ltm) as oiid_ltm
  from
  (
    select concat(oiid, '=', oiid_ltm) as oiid_concat
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as m
  lateral view explode_tags(oiid_concat) mytable as oiid, oiid_ltm
  group by oiid
) as n
group by oiid,oiid_ltm
)c
)a
join
(select oiid_history as oiid_history_yesterday,oiid_yearhalf as oiid_yearhalf_yesterday,oiid_two_year as oiid_two_year_yesterday
from mobdi_muid_dashboard.oiid_all_dau_by_dim_device_oiid_merge_df
where day='$lastday')b
on 1=1
;"


