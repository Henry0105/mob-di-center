#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g';
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

drop table if exists mobdi_test.gai_jh_mac_all;
create table mobdi_test.gai_jh_mac_all stored as orc as
select device, 
concat_ws(',', collect_list(mac)) as mac, 
concat_ws(',', collect_list(mac_ltm)) as mac_ltm
from 
(
    select device,
      CASE
        WHEN bb.value IS NOT NULL THEN null
        WHEN aa.mac is not null and aa.mac <> '' then aa.mac
        ELSE null
      END as mac,
      CASE
        WHEN bb.value IS NOT NULL THEN null
        WHEN aa.mac is not null and aa.mac <> '' then aa.mac_ltm
        ELSE null
      END as mac_ltm
	from
    (
      select device, mac, cast(max(serdatetime) as string) as mac_ltm
      from 
      (
        select device, mytable.mac_split as mac, serdatetime
        from 
        (
          select device, mac, serdatetime
          from mobdi_test.gai_device_phone_imei_mac_2020_after_1
          where mac is not null and lower(trim(mac)) not in ('', '02:00:00:00:00:00', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
    	  union all
    	  select device, mac, serdatetime
          from mobdi_test.gai_device_phone_imei_mac_2020_pre_1
          where mac is not null and lower(trim(mac)) not in ('', '02:00:00:00:00:00', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
        ) as a 
        lateral view explode(split(mac, ',')) mytable as mac_split
        where mytable.mac_split is not null and trim(mytable.mac_split) not in ('', '02:00:00:00:00:00', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
      ) as b
      group by device, mac
    )aa left join(
      SELECT lower(value) as value 
      FROM dm_sdk_mapping.blacklist 
      where type='mac' and day='20180702' 
      GROUP BY lower(value)
    )bb on aa.mac=bb.value
) as c 
group by device;
"