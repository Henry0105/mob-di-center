#!/bin/bash

set -e -x

: '
@owner:baron
@describe:oiid,ieid,pid,isid匹配率近一年半 
@projectName:mobdi_muid_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1
yearhalf_days=`date -d "$day -540 days" +%Y%m%d`

:<<!
create table if not exists mobdi_muid_dashboard.other_id_match_yearhalf_by_dim_device_merge_df(
ieid_pid_match_yearhalf bigint comment 'ieid_pid匹配率 近一年半',
pid_ieid_match_yearhalf bigint comment'pid_ieid匹配率 近一年半',
oiid_pid_match_yearhalf bigint comment 'oiid_pid匹配率 近一年半',
pid_oiid_match_yearhalf bigint comment'pid_oiid匹配率 近一年半',
ieid_oiid_match_yearhalf bigint comment 'ieid_oiid匹配率 近一年半',
oiid_ieid_match_yearhalf bigint comment'oiid_ieid匹配率 近一年半',
)comment 'oiid,ieid,pid,匹配率历史全量'
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
insert overwrite table mobdi_muid_dashboard.other_id_match_yearhalf_by_dim_device_merge_df partition(day=$day)
select
ieid_pid_match_yearhalf,pid_ieid_match_yearhalf,oiid_pid_match_yearhalf,pid_oiid_match_yearhalf,ieid_oiid_match_yearhalf,oiid_ieid_match_yearhalf
from
(select
count(*) as ieid_pid_match_yearhalf
from
(
select ieid
from 
(
  select ieid_concat
  from 
  (
    select device, concat(ieid, '=', ieid_ltm) as ieid_concat
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as a 
  left semi join 
  (
    select device
    from 
    (
      select device, concat(pid, '=', pid_ltm) as pid_concat
      from dim_mobdi_mapping.dim_device_pid_merge_df
      where day = '$day' and length(pid) > 0
    ) as m
    lateral view explode_tags(pid_concat) mytable as pid, pid_ltm
    where from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by device
  ) as b 
  on a.device = b.device
) as c 
lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
where from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
group by ieid
)d
)a1
join
(
select
count(*) as pid_ieid_match_yearhalf
from
(
select pid
from 
(
  select pid_concat
  from 
  (
    select device, concat(pid, '=', pid_ltm) as pid_concat
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as a 
  left semi join 
  (
    select device
    from 
    (
      select device, concat(ieid, '=', ieid_ltm) as ieid_concat
      from dim_mobdi_mapping.dim_device_ieid_merge_df
      where day = '$day' and length(ieid) > 0
    ) as m
    lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
    where from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by device
  ) as b 
  on a.device = b.device
) as c 
lateral view explode_tags(pid_concat) mytable as pid, pid_ltm
where from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
group by pid
)d
)a2
join
(
select count(*) as oiid_pid_match_yearhalf
from 
(
select oiid
from 
(
  select oiid_concat
  from 
  (
    select device, concat(oiid, '=', oiid_ltm) as oiid_concat
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as a 
  left semi join 
  (
    select device
    from 
    (
      select device, concat(pid, '=', pid_ltm) as pid_concat
      from dim_mobdi_mapping.dim_device_pid_merge_df
      where day = '$day' and length(pid) > 0
    ) as m
    lateral view explode_tags(pid_concat) mytable as pid, pid_ltm
    where from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by device
  ) as b 
  on a.device = b.device
) as c 
lateral view explode_tags(oiid_concat) mytable as oiid, oiid_ltm
where from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
group by oiid
)d
)a3
join
(
select
count(*) as pid_oiid_match_yearhalf
from
(
select pid
from 
(
  select pid_concat
  from 
  (
    select device, concat(pid, '=', pid_ltm) as pid_concat
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as a 
  left semi join 
  (
    select device
    from 
    (
      select device, concat(oiid, '=', oiid_ltm) as oiid_concat
      from dim_mobdi_mapping.dim_device_oiid_merge_df
      where day = '$day' and length(oiid) > 0
    ) as m
    lateral view explode_tags(oiid_concat) mytable as oiid, oiid_ltm
    where from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by device
  ) as b 
  on a.device = b.device
) as c 
lateral view explode_tags(pid_concat) mytable as pid, pid_ltm
where from_unixtime(cast(pid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
group by pid
)d
)a4
join
(select
count(*) as ieid_oiid_match_yearhalf
from
(
select ieid
from 
(
  select ieid_concat
  from 
  (
    select device, concat(ieid, '=', ieid_ltm) as ieid_concat
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as a 
  left semi join 
  (
    select device
    from 
    (
      select device, concat(oiid, '=', oiid_ltm) as oiid_concat
      from dim_mobdi_mapping.dim_device_oiid_merge_df
      where day = '$day' and length(oiid) > 0
    ) as m
    lateral view explode_tags(oiid_concat) mytable as oiid, oiid_ltm
    where from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by device
  ) as b 
  on a.device = b.device
) as c 
lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
where from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
group by ieid
)d
)a5
join
(
select count(*) as oiid_ieid_match_yearhalf
from
(
select oiid
from 
(
  select oiid_concat
  from 
  (
    select device, concat(oiid, '=', oiid_ltm) as oiid_concat
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as a 
  left semi join 
  (
    select device
    from 
    (
      select device, concat(ieid, '=', ieid_ltm) as ieid_concat
      from dim_mobdi_mapping.dim_device_ieid_merge_df
      where day = '$day' and length(ieid) > 0
    ) as m
    lateral view explode_tags(ieid_concat) mytable as ieid, ieid_ltm
    where from_unixtime(cast(ieid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
    group by device
  ) as b 
  on a.device = b.device
) as c 
lateral view explode_tags(oiid_concat) mytable as oiid, oiid_ltm
where from_unixtime(cast(oiid_ltm as bigint), 'yyyyMMdd') >= $yearhalf_days
group by oiid
)d
)a6
"
