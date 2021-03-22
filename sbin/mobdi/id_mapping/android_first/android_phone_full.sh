#!/bin/sh
#phone不在限制个数

set -x -e

:<<!
取dm_mobdi_mapping.phone_mapping_full最后一个分区的全量数据与dw_mobdi_md.phone_mapping_incr的分区数据进行全量更新
!

# input
dws_phone_mapping_di=dm_mobdi_topic.dws_phone_mapping_di

# output
dim_phone_mapping_df=dm_mobdi_mapping.dim_phone_mapping_df



HADOOP_USER_NAME=dba hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
set mapreduce.job.queuename=root.yarn_data_compliance2;

insert overwrite table $dim_phone_mapping_df partition (version='20151231.1000',plat=1)
select device,
phoneno,
phoneno_tm,
phoneno_tm as phoneno_ltm,
ext_phoneno,
ext_phoneno_tm,
ext_phoneno_tm as ext_phoneno_ltm,
sms_phoneno,
sms_phoneno_tm,
sms_phoneno_tm as sms_phoneno_ltm,
imsi,
imsi_tm,
imsi_tm as imsi_ltm,
ext_imsi,
ext_imsi_tm,
ext_imsi_tm as ext_imsi_ltm,
mobauth_phone,
mobauth_phone_tm,
mobauth_phone_tm as mobauth_phone_ltm
from $dws_phone_mapping_di where day='20151231' and plat=1
"
