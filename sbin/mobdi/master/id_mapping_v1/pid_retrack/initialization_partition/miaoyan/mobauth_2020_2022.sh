#!/bin/bash

set -x -e


HADOOP_USER_NAME=dba hive -v -e"
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx6144m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set mapreduce.job.queuename=root.yarn_data_compliance;

drop table if exists mobdi_test.gai_device_phone_2020_after;
create table mobdi_test.gai_device_phone_2020_after stored as orc as
select device, phone, day, max(datetime) as datetime
from
(
  select t2.device as device, phone, day, datetime
  from
  (
    select duid, concat('000860', phone) as phone, day,
    cast(max(datetime)/1000 as int) as datetime
    from dw_sdk_log.mobauth_operator_auth
    where plat = 1 and duid is not null and phone is not null
    and length(duid) = 40 and length(phone) = 11
    and phone = regexp_extract(phone, '1[2-9][0-9]{9}', 0)
    and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
    and day < '20220301' and day>='20210101'
    group by duid, phone, day
  ) as t1
  left join
  (
    select muid as device, duid
    from dw_sdk_log.mobauth_pvlog
    where plat = 1 and deviceid is not null and duid is not null
    and length(muid) = 40 and length(duid) = 40
    and duid = regexp_extract(duid, '[0-9a-f]{40}', 0)
    and muid = regexp_extract(muid, '[0-9a-f]{40}', 0)
    and day < '20220301' and day>='20210101'
    group by muid, duid
  ) as t2
  on t1.duid = t2.duid
  where t2.device is not null

  union all

  select muid as device,
  concat('000860', phone) as phone, day,
  cast(max(datetime)/1000 as int) as datetime
  from dw_sdk_log.mobauth_operator_login
  where day <'20220301' and day>='20210101'
  and plat = 1 and muid is not null
  and length(muid) = 40
  and muid = regexp_extract(muid, '[0-9a-f]{40}', 0)
  group by muid, phone, day
) as a
group by device, phone, day;

"