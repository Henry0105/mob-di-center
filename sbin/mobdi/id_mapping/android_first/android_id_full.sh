#!/bin/sh

set -e -x

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!


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


insert overwrite table $dim_id_mapping_android_df partition (version='20151231.1000')
select
    device,
    mac,
    mac_tm,
    mac_tm as mac_ltm,
    macarray,
    null as  macarray_tm,
    imei,
    imei_tm,
    imei_tm as imei_ltm,
    imeiarray,
    null as imeiarray_tm,
    serialno,
    serialno_tm,
    serialno_tm as serialno_ltm,
    adsid,
    adsid_tm,
    adsid_tm as adsid_ltm,
    androidid,
    androidid_tm,
    androidid_tm as androidid_ltm,
    simserialno,
    simserialno_tm,
    simserialno_tm as simserialno_ltm,
    imsi,
    imsi_tm,
    imsi_tm as imsi_ltm,
    imsiarray,
    null as imsiarray_tm,
    phone,
    phone_tm,
    phone_tm as phone_ltm,
    snsuid_list,
    null as snsuid_list_tm,
    ARRAY_DISTINCT(carrierarray) as carrierarray,
    orig_imei,
    orig_imei_tm,
    orig_imei_tm as orig_imei_ltm,
    oaid,
    oaid_tm,
    oaid_tm as oaid_ltm
from $dwd_id_mapping_android_di where day='20151231'
"