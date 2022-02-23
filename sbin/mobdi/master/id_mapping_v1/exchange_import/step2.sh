#!/bin/bash

set -e -x


# input
i_p=mobdi_test.i_p
dim_ieid_transform_full_par_sec=dm_mobdi_mapping.dim_ieid_transform_full_par_sec

# output
imei14_exchange=mobdi_test.android_imei14_exchange


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




---step2
drop table if exists $imei14_exchange;
create table $imei14_exchange stored as orc as
select ieid,
        concat_ws(',',collect_list(phone)) as phone_set,
        concat_ws(',',collect_list(phone_tm)) as phone_tm_set
from
(
    select
        d.ieid as ieid,
        pp.phone as phone,
        pp.phone_ltm as phone_tm
    from
    (
      select imei14, phone, phone_ltm from $i_p
    ) pp
    inner join
    (
        select ieid14_lower, ieid
        from $dim_ieid_transform_full_par_sec
        where ieid14_lower is not null and length(ieid14_lower)>0
        group by ieid14_lower, ieid
    ) d
    on d.ieid14_lower = concat_ws(',',Md5EncryptArray(split(lower(trim(pp.imei14)), ',')))
) ieid_14
group by ieid;
"
