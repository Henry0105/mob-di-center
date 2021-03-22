#!/bin/sh

set -e -x

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi


day=$1

# 获取当前日期的下个月第一天
nextmonth=`date -d "${day} +1 month" +%Y%m01`
# 获取当前日期所在月的第一天
startdate=`date -d"${day}" +%Y%m01`
# 获取当前日期所在月的最后一天
enddate=`date -d "$nextmonth last day" +%Y%m%d`


# input
dwd_id_mapping_android_di=dm_mobdi_topic.dws_id_mapping_android_di

# output
dim_id_mapping_android_df=dm_mobdi_mapping.dim_id_mapping_android_df


# 获取增量表和全量表的更新时间的最小值
max_tm() {
a_tm=$1
b_tm=$2
echo "
if(${a_tm} < ${b_tm}, ${b_tm}, ${a_tm})
"
}

# 获取增量表和全量表的更新时间的最小值
min_tm() {
a_tm=$1
b_tm=$2
echo "
if(${a_tm} > ${b_tm}, ${b_tm}, ${a_tm})
"
}

# 方法解读：
# 1.如果全量表中mcidarray=null or size(mcidarray) <= 0, 直接输出增量表中的 mcidarray
# 2.如果增量表中mcidarray=null or size(mcidarray) <= 0, 接着判断全量表中 size(mcidarray) <= 0
# 3.如果全量表和增量表中的size都>0，返回一个并集（去重）
update_array() {
f="$1"
echo "
case
    when a.$f is null or size(a.$f) <= 0 then b.$f
    when b.$f is null or size(b.$f) <= 0 then if(size(a.$f) <= 0, null, a.$f)
else array_union(a.$f, b.$f)
end as $f
"
}

# 1. 如果增量表中的字段有些值不在全量表中,则选取增量表中该字段的更新时间和全量表的更新时间二者较大的
# 2. 如果增量表中的字段都在全量表中
# 2.1 如果二者字段相同,则选择两个更新时间较小的
# 2.2 否则使用全量表中的更新时间
update_array_tm() {
f="$1"
max_tm_s=`max_tm "${f}_tm" 'b.day'`
min_tm_s=`min_tm "${f}_tm" 'b.day'`
echo "
if (a.device = b.device,
    if(size(array_diff(b.$f, a.$f)) > 0,
        ${max_tm_s},
        if(size(b.$f) = size(a.$f), ${min_tm_s}, ${f}_tm)),
    coalesce(b.day, ${f}_tm)
) as ${f}_tm
"
}

full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_id_mapping_android_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

HADOOP_USER_NAME=dba hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function array_union as 'com.youzu.mob.java.udf.ArrayUnionUDF';
create temporary function array_diff as 'com.youzu.mob.java.udf.ArrayDiffUDF';
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';
create temporary function ARRAY_DISTINCT as 'com.youzu.mob.java.udf.ArrayDistinct';
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3860m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3860m';
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts='-Xmx10240m';

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set mapreduce.job.queuename=root.yarn_data_compliance2;


insert overwrite table $dim_id_mapping_android_df partition (version='${enddate}.1000')
select
    coalesce(b.device,a.device) as device,
    mobdi_array_udf('field', a.mac, a.mac_tm,  b.mac, b.mac_tm,'min', '/hiveDW/dm_mobdi_md/mac_blacklist/') as mac,
    mobdi_array_udf('date', a.mac, a.mac_tm, b.mac, b.mac_tm,'min', '/hiveDW/dm_mobdi_md/mac_blacklist/') as mac_tm,
    mobdi_array_udf('date', a.mac, a.mac_ltm, b.mac, b.mac_tm,'max', '/hiveDW/dm_mobdi_md/mac_blacklist/') as mac_ltm,
    `update_array 'macarray'`,
    `update_array_tm 'macarray'`,
    mobdi_array_udf('field', lower(a.imei), a.imei_tm, lower(b.imei), b.imei_tm,'min','/hiveDW/dm_mobdi_md/imei_blacklist/') as imei,
    mobdi_array_udf('date', lower(a.imei), a.imei_tm,   lower(b.imei), b.imei_tm,'min','/hiveDW/dm_mobdi_md/imei_blacklist/') as imei_tm,
    mobdi_array_udf('date', lower(a.imei), a.imei_ltm,  lower(b.imei), b.imei_tm,'max','/hiveDW/dm_mobdi_md/imei_blacklist/') as imei_ltm,
    `update_array 'imeiarray'`,
    `update_array_tm 'imeiarray'`,
    mobdi_array_udf('field', a.serialno, a.serialno_tm,  b.serialno, b.serialno_tm,'min', '/hiveDW/dm_mobdi_md/serialno_blacklist/') as serialno,
    mobdi_array_udf('date', a.serialno, a.serialno_tm,  b.serialno, b.serialno_tm,'min',  '/hiveDW/dm_mobdi_md/serialno_blacklist/') as serialno_tm,
    mobdi_array_udf('date', a.serialno, a.serialno_ltm, b.serialno, b.serialno_tm,'max',  '/hiveDW/dm_mobdi_md/serialno_blacklist/') as serialno_ltm,

    mobdi_array_udf('field', a.adsid, a.adsid_tm, b.adsid, b.adsid_tm,'min') as adsid,
    mobdi_array_udf('date', a.adsid, a.adsid_tm, b.adsid, b.adsid_tm,'min') as adsid_tm,
    mobdi_array_udf('date', a.adsid, a.adsid_ltm,b.adsid, b.adsid_tm,'max') as adsid_ltm,

    mobdi_array_udf('field', a.androidid, a.androidid_tm,b.androidid, b.androidid_tm,'min',  '/hiveDW/dm_mobdi_md/androidid_blacklist/') as androidid,
    mobdi_array_udf('date', a.androidid, a.androidid_tm, b.androidid, b.androidid_tm,'min',  '/hiveDW/dm_mobdi_md/androidid_blacklist/') as androidid_tm,
    mobdi_array_udf('date', a.androidid, a.androidid_ltm, b.androidid, b.androidid_tm,'max', '/hiveDW/dm_mobdi_md/androidid_blacklist/') as androidid_ltm,

    mobdi_array_udf('field', a.simserialno, a.simserialno_tm,b.simserialno, b.simserialno_tm,'min', '/hiveDW/dm_mobdi_md/simserialno_blacklist/') as simserialno,
    mobdi_array_udf('date', a.simserialno, a.simserialno_tm, b.simserialno, b.simserialno_tm,'min', '/hiveDW/dm_mobdi_md/simserialno_blacklist/') as simserialno_tm,
    mobdi_array_udf('date', a.simserialno, a.simserialno_ltm,b.simserialno, b.simserialno_tm,'max', '/hiveDW/dm_mobdi_md/simserialno_blacklist/') as simserialno_ltm,

    mobdi_array_udf('field', a.imsi, a.imsi_tm, b.imsi, b.imsi_tm,'min', '/hiveDW/dm_mobdi_md/imsi_blacklist/') as imsi,
    mobdi_array_udf('date', a.imsi, a.imsi_tm, b.imsi, b.imsi_tm,'min',  '/hiveDW/dm_mobdi_md/imsi_blacklist/') as imsi_tm,
    mobdi_array_udf('date', a.imsi, a.imsi_ltm, b.imsi, b.imsi_tm,'max', '/hiveDW/dm_mobdi_md/imsi_blacklist/') as imsi_ltm,

    `update_array 'imsiarray'`,
    `update_array_tm 'imsiarray'`,

     mobdi_array_udf('field', a.phone, a.phone_tm,b.phone, b.phone_tm,'min','/hiveDW/dm_mobdi_md/phone2_blacklist/')as phone,
     mobdi_array_udf('date', a.phone, a.phone_tm,b.phone, b.phone_tm,'min','/hiveDW/dm_mobdi_md/phone2_blacklist/') as phone_tm,
     mobdi_array_udf('date', a.phone, a.phone_ltm, b.phone, b.phone_tm,'max','/hiveDW/dm_mobdi_md/phone2_blacklist/') as phone_ltm,

    if(a.device = b.device, if(snsuid_list_tm < b.day, b.snsuid_list, a.snsuid_list), coalesce(b.snsuid_list, a.snsuid_list)) snsuid_list,
    if(a.device = b.device, if(snsuid_list_tm < b.day, b.day, snsuid_list_tm), coalesce(b.day, snsuid_list_tm)) as snsuid_list_tm,
    ARRAY_DISTINCT(a.carrierarray, b.carrierarray) as carrierarray,

    mobdi_array_udf('field', lower(a.orig_imei), a.orig_imei_tm, lower(b.orig_imei), b.orig_imei_tm,'min','/hiveDW/dm_mobdi_md/imei_blacklist/') as orig_imei,
    mobdi_array_udf('date', lower(a.orig_imei), a.orig_imei_tm,   lower(b.orig_imei), b.orig_imei_tm,'min','/hiveDW/dm_mobdi_md/imei_blacklist/') as orig_imei_tm,
    mobdi_array_udf('date', lower(a.orig_imei), a.orig_imei_ltm,  lower(b.orig_imei), b.orig_imei_tm,'max','/hiveDW/dm_mobdi_md/imei_blacklist/') as orig_imei_ltm,

    mobdi_array_udf('field', a.oaid, a.oaid_tm, b.oaid, b.oaid_tm,'min') as oaid,
    mobdi_array_udf('date', a.oaid, a.oaid_tm, b.oaid, b.oaid_tm,'min') as oaid_tm,
    mobdi_array_udf('date', a.oaid, a.oaid_ltm, b.oaid, b.oaid_tm,'max') as oaid_ltm
from
(
    select *
    from $dim_id_mapping_android_df
    where version = '$full_last_version'
    and device is not null
    and length(device)= 40
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) a
full join
(
    select *
    from $dwd_id_mapping_android_di
    where day='$enddate'
) b
on a.device = b.device
"
