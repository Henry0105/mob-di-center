#!/bin/sh
#phone不在限制个数

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1
# 获取当前日期的下个月第一天
nextmonth=`date -d "${insert_day} +1 month" +%Y%m01`
# 获取当前日期所在月的最后一天
enddate=`date -d "$nextmonth last day" +%Y%m%d`



:<<!
取dm_mobdi_mapping.phone_mapping_full最后一个分区的全量数据与dw_mobdi_md.phone_mapping_incr的分区数据进行全量更新
!

# input
dws_phone_mapping_di=dm_mobdi_topic.dws_phone_mapping_di

# output
dim_phone_mapping_df=dm_mobdi_mapping.dim_phone_mapping_df


full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_phone_mapping_df', 'version');
drop temporary function GET_LAST_PARTITION;
"

full_last_version=(`hive -e "$full_partition_sql"`)

ADOOP_USER_NAME=dba Hhive -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;
set mapred.job.reuse.jvm.num.tasks=10;

set mapreduce.job.queuename=root.yarn_data_compliance2;


insert overwrite table $dim_phone_mapping_df partition (version='${enddate}.1000',plat=1)
select
    coalesce(a.device, b.device) as device,
    mobdi_array_udf('field', a.phoneno, a.phoneno_tm, b.phoneno, b.phoneno_tm,'min', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as phoneno,
    mobdi_array_udf('date', a.phoneno, a.phoneno_tm, b.phoneno, b.phoneno_tm,'min',  '/hiveDW/dm_mobdi_md/phone2_blacklist/') as phoneno_tm,
    mobdi_array_udf('date', a.phoneno, a.phoneno_ltm, b.phoneno, b.phoneno_tm,'max', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as phoneno_ltm,

    mobdi_array_udf('field', a.ext_phoneno, a.ext_phoneno_tm, b.ext_phoneno, b.ext_phoneno_tm,'min', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as ext_phoneno,
    mobdi_array_udf('date', a.ext_phoneno, a.ext_phoneno_tm, b.ext_phoneno, b.ext_phoneno_tm,'min',  '/hiveDW/dm_mobdi_md/phone2_blacklist/') as ext_phoneno_tm,
    mobdi_array_udf('date', a.ext_phoneno, a.ext_phoneno_ltm, b.ext_phoneno, b.ext_phoneno_tm,'max', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as ext_phoneno_ltm,

    mobdi_array_udf('field', a.sms_phoneno, a.sms_phoneno_tm, b.sms_phoneno, b.sms_phoneno_tm,'min', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as sms_phoneno,
    mobdi_array_udf('date', a.sms_phoneno, a.sms_phoneno_tm, b.sms_phoneno, b.sms_phoneno_tm,'min',  '/hiveDW/dm_mobdi_md/phone2_blacklist/') as sms_phoneno_tm,
    mobdi_array_udf('date', a.sms_phoneno, a.sms_phoneno_ltm, b.sms_phoneno, b.sms_phoneno_tm,'max', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as sms_phoneno_ltm,

    mobdi_array_udf('field', a.imsi, a.imsi_tm, b.imsi, b.imsi_tm,'min', '/hiveDW/dm_mobdi_md/imsi_blacklist/') as imsi,
    mobdi_array_udf('date', a.imsi, a.imsi_tm, b.imsi, b.imsi_tm,'min',  '/hiveDW/dm_mobdi_md/imsi_blacklist/') as imsi_tm,
    mobdi_array_udf('date', a.imsi, a.imsi_ltm, b.imsi, b.imsi_tm,'max', '/hiveDW/dm_mobdi_md/imsi_blacklist/') as imsi_ltm,

    mobdi_array_udf('field', a.ext_imsi, a.ext_imsi_tm, b.ext_imsi, b.ext_imsi_tm,'min', '/hiveDW/dm_mobdi_md/imsi_blacklist/') as ext_imsi,
    mobdi_array_udf('date', a.ext_imsi, a.ext_imsi_tm, b.ext_imsi, b.ext_imsi_tm,'min',  '/hiveDW/dm_mobdi_md/imsi_blacklist/') as ext_imsi_tm,
    mobdi_array_udf('date', a.ext_imsi, a.ext_imsi_ltm, b.ext_imsi, b.ext_imsi_tm,'max', '/hiveDW/dm_mobdi_md/imsi_blacklist/') as ext_imsi_ltm,

    mobdi_array_udf('field', 'a.mobauth_phone', 'a.mobauth_phone_tm', b.mobauth_phone, b.mobauth_phone_tm,'min', '/hiveDW/dm_mobdi_md/phone2_blacklist/') as mobauth_phone,
    mobdi_array_udf('date', 'a.mobauth_phone', 'a.mobauth_phone_tm', b.mobauth_phone, b.mobauth_phone_tm,'min',  '/hiveDW/dm_mobdi_md/phone2_blacklist/') as mobauth_phone_tm,
    mobdi_array_udf('date', 'a.mobauth_phone', 'a.mobauth_phone_tm', b.mobauth_phone, b.mobauth_phone_tm,'max',  '/hiveDW/dm_mobdi_md/phone2_blacklist/') as mobauth_phone_ltm
from (
  select *
  from $dim_phone_mapping_df
  where version = '$full_last_version'
  and plat=1
  and device is not null
  and length(device)= 40
  and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) a
full join
(
  select *
  from $dws_phone_mapping_di
  where day='$enddate'
  and plat=1
) b
on a.device = b.device
"