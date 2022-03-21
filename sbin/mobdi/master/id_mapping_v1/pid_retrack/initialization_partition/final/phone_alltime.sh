#!/bin/bash

set -x -e

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=30;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function pid_encrypt as 'com.mob.udf.PidEncrypt';

insert overwrite table  mobdi_test.gai_pid_all partition(day='$day')
select device,pid,plat,time
from(
select device,pid,plat,time,
			row_number() over(partition by device,pid order by time desc) as rn
      from ( select device,pid,plat,time
             from ( select device,pid_encrypt(phone) as pid,1 as plat,
			        case when time='' or time is null then unix_timestamp('20220228','yyyyMMdd') else time end as time
                    from mobdi_test.gai_pid_all_1
                    where phone is not null and phone <> '' 
					and device is not null  and device <> '' and device <> '0000000000000000000000000000000000000000'
                    union all
                    select device,pid,plat,time
                    from ( select ifid1 as device,pid1 as pid,2 as plat,
                           case when pid_tm1='' or pid_tm1 is null then unix_timestamp(day,'yyyyMMdd') else pid_tm1 end as time
                           from dm_mobdi_master.dwd_ios_id_mapping_sec_di
                           lateral view explode_tags(concat(pid, '=', pid_tm)) mytable as pid1, pid_tm1
                           lateral view explode(split(ifid,',')) ifids as ifid1
                           where day = '20220228' and pid is not null and pid <> ''
                          ) t
                    where pid <> '' and pid is not null
                    and device is not null and device <> '' and device <> '00000000-0000-0000-0000-000000000000'
                  )  tt
                  group by device,pid,plat,time
           ) ttt
)tttt	
where rn = 1;
"


HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
set mapreduce.map.memory.mb=8096;
set mapreduce.map.java.opts='-Xmx6g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8096;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=30;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function string_sub_str as 'com.youzu.mob.mobdi.StringSubStr';


drop table if exists mobdi_test.gai_pid_all;
create table mobdi_test.gai_pid_all stored as orc as
select device, phone, time
from 
(
  select device, string_sub_str(phoneno) as phone, cast(serdatetime as string) as time
  from mobdi_test.gai_device_phone_imei_mac_2020_after     ---金华4185876717
  where phoneno is not null and trim(phoneno) <> ''
  union all 
  select device, string_sub_str(phoneno) as phone, cast(serdatetime as string) as time
  from mobdi_test.gai_device_phone_imei_mac_2020_pre     ---金华4106127877
  where phoneno is not null and trim(phoneno) <> ''
  union all 
  select device, string_sub_str(phoneno) as phone, cast(serdatetime as string) as time
  from mobdi_test.gai_device_phone_imei_mac_2020_after_0 
  where phoneno is not null and trim(phoneno) <> ''    
  union all 
  select device, string_sub_str(ext_phoneno) as phone, cast(ext_phoneno_ltm as string) as time
  from mobdi_test.gai_external_all    ---外采   ---38716243449
  where string_sub_str(ext_phoneno) is not null and string_sub_str(ext_phoneno) <> ''
  union all 
  select device, string_sub_str(phone) as phone, cast(datetime as string) as time
  from mobdi_test.gai_mobauth   ---秒验672503235
  where phone is not null and trim(phone) <> ''
  union all 
  select device, string_sub_str(phone) as phone, cast(serdatetime as string) as time
  from mobdi_test.gaizi_sms_phone    ---短信177468923
  where phone is not null and trim(phone) <> ''
) as a 
group by device, phone, time;
"
