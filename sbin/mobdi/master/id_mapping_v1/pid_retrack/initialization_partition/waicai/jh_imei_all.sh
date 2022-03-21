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

drop table if exists mobdi_test.gai_jh_imei_all;
create table mobdi_test.gai_jh_imei_all stored as orc as
select device, 
concat_ws(',', collect_list(imei)) as imei, 
concat_ws(',', collect_list(imei_ltm)) as imei_ltm
from 
(
    select device,
      CASE
        WHEN bb.value IS NOT NULL THEN null
        WHEN aa.imei is not null and aa.imei <> '' then aa.imei
        ELSE null
      END as imei,
        CASE
        WHEN bb.value IS NOT NULL THEN null
        WHEN aa.imei is not null and aa.imei <> '' then aa.imei_ltm
        ELSE null
      END as imei_ltm
    from(
      select device, imei, cast(max(serdatetime) as string) as imei_ltm
      from 
      (
        select device, mytable.imei_split as imei, serdatetime
        from 
        (
          select device, imei, serdatetime
          from mobdi_test.gai_device_phone_imei_mac_2020_after_1
          where imei is not null and trim(imei) not in ('', '000000000000000', '00000000000000') and size(split(imei, ',')) < 10000
    	  union all
    	  select device, imei, serdatetime
          from mobdi_test.gai_device_phone_imei_mac_2020_pre_1
          where imei is not null and trim(imei) not in ('', '000000000000000', '00000000000000') and size(split(imei, ',')) < 10000
        ) as a 
        lateral view explode(split(imei, ',')) mytable as imei_split
        where mytable.imei_split is not null and trim(mytable.imei_split) not in ('', '000000000000000', '00000000000000')
      ) as b 
      group by device, imei
    )aa left join(
        SELECT lower(value) as value 
        FROM dm_sdk_mapping.blacklist 
        where type='imei' and day='20180702' 
        GROUP BY lower(value)
    )bb
    on aa.imei=bb.value
) as c
group by device;
"