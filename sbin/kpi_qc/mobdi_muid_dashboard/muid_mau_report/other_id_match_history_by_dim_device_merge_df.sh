

#!/bin/bash

set -e -x

: '
@owner:baron
@describe:oiid,ieid,pid,isid匹配率 历史全量
@projectName:mobdi_muid_dashboard
'

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi

day=$1

:<<!
create table if not exists mobdi_muid_dashboard.other_id_match_history_by_dim_device_merge_df(
ieid_pid_match_history bigint comment 'ieid_pid匹配率 历史全量',
pid_ieid_match_history bigint comment'pid_ieid匹配率 历史全量',
oiid_pid_match_history bigint comment 'oiid_pid匹配率 历史全量',
pid_oiid_match_history bigint comment'pid_oiid匹配率 历史全量',
ieid_oiid_match_history bigint comment 'ieid_oiid匹配率 历史全量',
oiid_ieid_match_history bigint comment'oiid_ieid匹配率 历史全量'
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
insert overwrite table mobdi_muid_dashboard.other_id_match_history_by_dim_device_merge_df partition(day='$day')
select
ieid_pid_match_history,pid_ieid_match_history,oiid_pid_match_history,pid_oiid_match_history,ieid_oiid_match_history,oiid_ieid_match_history
from
(
select
count(*) as ieid_pid_match_history
from
(
select ieid_split as ieid
from 
(
  select a.device, ieid
  from 
  (
    select device, ieid
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as a 
  left semi join 
  (
    select device
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as b 
  on a.device = b.device
) as c 
lateral view explode(split(ieid, ',')) mytable as ieid_split
group by ieid_split
)d
)a1
join
(select
count(*) as pid_ieid_match_history
from
(
select pid_split as pid
from 
(
  select a.device, pid
  from 
  (
    select device, pid
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as a 
  left semi join 
  (
    select device
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as b 
  on a.device = b.device
) as c 
lateral view explode(split(pid, ',')) mytable as pid_split
group by pid_split
)d
)a2
join
(select
count(*) as oiid_pid_match_history
from
(
select oiid_split as oiid
from 
(
  select a.device, oiid
  from 
  (
    select device, oiid
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as a 
  left semi join 
  (
    select device
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as b 
  on a.device = b.device
) as c 
lateral view explode(split(oiid, ',')) mytable as oiid_split
group by oiid_split
)d
)a3
join
(
select count(*) as pid_oiid_match_history
from
(
select pid_split as pid
from 
(
  select a.device, pid
  from 
  (
    select device, pid
    from dim_mobdi_mapping.dim_device_pid_merge_df
    where day = '$day' and length(pid) > 0
  ) as a 
  inner join 
  (
    select device
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as b 
  on a.device = b.device
) as c 
lateral view explode(split(pid, ',')) mytable as pid_split
group by pid_split
)d
)a4
join
(
select count(*) as ieid_oiid_match_history
from
(
select ieid_split as ieid
from 
(
  select a.device, ieid
  from 
  (
    select device, ieid
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as a 
  inner join 
  (
    select device
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as b 
  on a.device = b.device
) as c 
lateral view explode(split(ieid, ',')) mytable as ieid_split
group by ieid_split
)d
)a5
join
(
select count(*) as oiid_ieid_match_history
from
(
select oiid_split as oiid
from 
(
  select a.device, oiid
  from 
  (
    select device, oiid
    from dim_mobdi_mapping.dim_device_oiid_merge_df
    where day = '$day' and length(oiid) > 0
  ) as a 
  inner join 
  (
    select device
    from dim_mobdi_mapping.dim_device_ieid_merge_df
    where day = '$day' and length(ieid) > 0
  ) as b 
  on a.device = b.device
) as c 
lateral view explode(split(oiid, ',')) mytable as oiid_split
group by oiid_split
)d
)a6;
"
