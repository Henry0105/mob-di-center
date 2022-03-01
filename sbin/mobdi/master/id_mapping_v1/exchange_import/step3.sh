#!/bin/bash

set -e -x

insert_day=$1

# input
imei14_exchange=mobdi_test.android_imei14_exchange
dim_device_id_abnormal_sec_df=dim_mobdi_mapping.dim_device_id_abnormal_sec_df
dim_device_ieid_merge_df=dim_mobdi_mapping.dim_device_ieid_merge_df

# output
ext_phoneno_v=mobdi_test.ext_phoneno_v


HADOOP_USER_NAME=dba hive -v -e "
SET hive.auto.convert.join=true;
SET hive.map.aggr=false;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx7370m';
set mapreduce.child.map.java.opts='-Xmx7370m';
set mapreduce.reduce.memory.mb=15360;
set mapreduce.reduce.java.opts='-Xmx13900m' -XX:+UseG1GC;
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;
set mapred.task.timeout=1800000;
set mapreduce.job.queuename=root.yarn_data_compliance;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;

create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';




---- step3
drop table if exists $ext_phoneno_v;
create table $ext_phoneno_v stored as orc as
select device,
       concat_ws(',',if(length(trim(phone_imei))=0,null,phone_imei)) as ext_phone,
       concat_ws(',',if(length(trim(phone_imei_tm))=0,null,phone_imei_tm)) as ext_phone_tm
from
(
  select
      android_incr_explode.device,
      concat_ws(',',collect_list(android_imei14_exchange_sec.phone_set)) as phone_imei,
      concat_ws(',',collect_list(android_imei14_exchange_sec.phone_tm_set)) as phone_imei_tm
  from
  (
      select
        device,ieid
      from
      (
          select
              ieid_full.device as device,
              t_ieid.ieid1 as ieid
          from
          (
             SELECT
                device,
                ieid
             FROM $dim_device_ieid_merge_df
                where day='${insert_day}'
                and length(trim(device)) = 40
                and ieid is not null and length(ieid)>0
          ) ieid_full
          lateral view explode(split(ieid_full.ieid, ',')) t_ieid as ieid1

          union all

          select device,id as ieid from $dim_device_id_abnormal_sec_df
          where day='${insert_day}' and flag='ieid'

      ) device_ieid_tmp
      group by device, ieid

  ) android_incr_explode
  left join
  (
    select
        ieid,
        phone_set,
        phone_tm_set
    from $imei14_exchange
  ) android_imei14_exchange_sec
  on android_incr_explode.ieid=android_imei14_exchange_sec.ieid
  group by android_incr_explode.device
)tt
where tt.phone_imei is not null and length(tt.phone_imei)>0;
"
