#!/bin/sh

set -x -e

: '
@owner:hushk,luost
@describe:android_id增量表更新
@projectName:mobdi
@BusinessName:id_mapping
'

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

#input
android_id_mapping_incr=dm_mobdi_master.dwd_android_id_mapping_sec_di

#output
android_id_mapping_full=dm_mobdi_mapping.android_id_mapping_sec_df

# 获取增量表和全量表的更新时间的最大值
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
        if(size(b.$f)=size(a.$f), ${min_tm_s}, ${f}_tm)),
    coalesce(b.day, ${f}_tm)
) as ${f}_tm
"
}

full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'android_id_mapping_sec_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)


hive -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function array_union as 'com.youzu.mob.java.udf.ArrayUnionUDF';
create temporary function array_diff as 'com.youzu.mob.java.udf.ArrayDiffUDF';
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';
create temporary function ARRAY_DISTINCT as 'com.youzu.mob.java.udf.ArrayDistinct';

set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx7400m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx7400m';
set mapreduce.reduce.memory.mb=15360;
SET mapreduce.reduce.java.opts='-Xmx13g';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;


insert overwrite table $android_id_mapping_full partition (version = '${day}.1000')
select coalesce(b.device,a.device) as device,
       mobdi_array_udf('field', a.mcid, a.mcid_tm, b.mcid, b.mcid_tm,'min', '/hiveDW/dm_mobdi_md/mcid_blacklist/') as mcid,
       mobdi_array_udf('date', a.mcid, a.mcid_tm, b.mcid, b.mcid_tm,'min', '/hiveDW/dm_mobdi_md/mcid_blacklist/') as mcid_tm,
       mobdi_array_udf('date', a.mcid, a.mcid_ltm, b.mcid, b.mcid_tm,'max', '/hiveDW/dm_mobdi_md/mcid_blacklist/') as mcid_ltm,
       `update_array 'mcidarray'`,
       `update_array_tm 'mcidarray'`,
       mobdi_array_udf('field', lower(a.ieid), a.ieid_tm, lower(b.ieid), b.ieid_tm,'min','/hiveDW/dm_mobdi_md/ieid_blacklist/') as ieid,
       mobdi_array_udf('date', lower(a.ieid), a.ieid_tm, lower(b.ieid), b.ieid_tm,'min','/hiveDW/dm_mobdi_md/ieid_blacklist/') as ieid_tm,
       mobdi_array_udf('date', lower(a.ieid), a.ieid_ltm, lower(b.ieid), b.ieid_tm,'max','/hiveDW/dm_mobdi_md/ieid_blacklist/') as ieid_ltm,
       `update_array 'ieidarray'`,
       `update_array_tm 'ieidarray'`,
       mobdi_array_udf('field', a.snid, a.snid_tm, b.snid, b.snid_tm,'min') as snid,
       mobdi_array_udf('date', a.snid, a.snid_tm, b.snid, b.snid_tm,'min') as snid_tm,
       mobdi_array_udf('date', a.snid, a.snid_ltm, b.snid, b.snid_tm,'max') as snid_ltm,
       mobdi_array_udf('field', a.asid, a.asid_tm, b.asid, b.asid_tm,'min') as asid,
       mobdi_array_udf('date', a.asid, a.asid_tm, b.asid, b.asid_tm,'min') as asid_tm,
       mobdi_array_udf('date', a.asid, a.asid_ltm, b.asid, b.asid_tm,'max') as asid_ltm,
       mobdi_array_udf('field', a.adrid, a.adrid_tm, b.adrid, b.adrid_tm,'min') as adrid,
       mobdi_array_udf('date', a.adrid, a.adrid_tm, b.adrid, b.adrid_tm,'min') as adrid_tm,
       mobdi_array_udf('date', a.adrid, a.adrid_ltm, b.adrid, b.adrid_tm,'max') as adrid_ltm,
       mobdi_array_udf('field', a.ssnid, a.ssnid_tm, b.ssnid, b.ssnid_tm,'min') as ssnid,
       mobdi_array_udf('date', a.ssnid, a.ssnid_tm, b.ssnid, b.ssnid_tm,'min') as ssnid_tm,
       mobdi_array_udf('date', a.ssnid, a.ssnid_ltm, b.ssnid, b.ssnid_tm,'max') as ssnid_ltm,
       mobdi_array_udf('field', a.isid, a.isid_tm, b.isid, b.isid_tm,'min') as isid,
       mobdi_array_udf('date', a.isid, a.isid_tm, b.isid, b.isid_tm,'min') as isid_tm,
       mobdi_array_udf('date', a.isid, a.isid_ltm, b.isid, b.isid_tm,'max') as isid_ltm,
       `update_array 'isidarray'`,
       `update_array_tm 'isidarray'`,
       mobdi_array_udf('field', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/')as pid,
       mobdi_array_udf('date', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/') as pid_tm,
       mobdi_array_udf('date', a.pid, a.pid_ltm, b.pid, b.pid_tm,'max','/hiveDW/dm_mobdi_md/pid_blacklist/') as pid_ltm,
       if(a.device = b.device, if(snsuid_list_tm < b.day, b.snsuid_list, a.snsuid_list), coalesce(b.snsuid_list, a.snsuid_list)) snsuid_list,
       if(a.device = b.device, if(snsuid_list_tm < b.day, b.day, snsuid_list_tm), coalesce(b.day, snsuid_list_tm)) as snsuid_list_tm,
       ARRAY_DISTINCT(a.carrierarray, b.carrierarray) as carrierarray,
       mobdi_array_udf('field', lower(a.orig_ieid), a.orig_ieid_tm, lower(b.orig_ieid), b.orig_ieid_tm,'min','/hiveDW/dm_mobdi_md/ieid_blacklist/') as orig_ieid,
       mobdi_array_udf('date', lower(a.orig_ieid), a.orig_ieid_tm,   lower(b.orig_ieid), b.orig_ieid_tm,'min','/hiveDW/dm_mobdi_md/ieid_blacklist/') as orig_ieid_tm,
       mobdi_array_udf('date', lower(a.orig_ieid), a.orig_ieid_ltm,  lower(b.orig_ieid), b.orig_ieid_tm,'max','/hiveDW/dm_mobdi_md/ieid_blacklist/') as orig_ieid_ltm,
       mobdi_array_udf('field', a.oiid, a.oiid_tm, b.oiid, b.oiid_tm,'min') as oiid,
       mobdi_array_udf('date', a.oiid, a.oiid_tm, b.oiid, b.oiid_tm,'min') as oiid_tm,
       mobdi_array_udf('date', a.oiid, a.oiid_ltm, b.oiid, b.oiid_tm,'max') as oiid_ltm
from
(
    select *
    from $android_id_mapping_full
    where version = '$full_last_version'
    and device is not null 
    and length(device)= 40 
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) a
full join
(
    select * 
    from $android_id_mapping_incr 
    where day = '$day'
) b
on a.device = b.device
"

# full表分区清理，保留最近10天数据，同时保留每月最后一天的数据
delete_day=`date +%Y%m%d -d "${insert_day} -10 day"`
#上个月
LastMonth=`date -d "last month" +"%Y%m"`
#这个月
_todayYM=`date +"%Y%m"`
#本月第一天
CurrentMonthFirstDay=$_todayYM"01"
#本月第一天时间戳
_CurrentMonthFirstDaySeconds=`date -d "$CurrentMonthFirstDay" +%s`
#上月最后一天时间戳
_LastMonthLastDaySeconds=`expr $_CurrentMonthFirstDaySeconds - 86400`
#上个月第一天
LastMonthFistDay=`date -d @$_LastMonthLastDaySeconds "+%Y%m"`"01"
#上个月最后一天
LastMonthLastDay=`date -d @$_LastMonthLastDaySeconds "+%Y%m%d"`

if [[ "$delete_day" -ne "$LastMonthLastDay" ]]; then
  # 保留每月最后一天的数据
  # do delete thing
  echo "deleting version: ${delete_day}.1000 if exists"
  hive -e "alter table dm_mobdi_mapping.android_id_mapping_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1000');"
  echo "deleting version: ${delete_day}.1001 if exists"
  hive -e "alter table dm_mobdi_mapping.android_id_mapping_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1001');"
  echo "deleting version: ${delete_day}.1002 if exists"
  hive -e "alter table dm_mobdi_mapping.android_id_mapping_sec_df DROP IF EXISTS PARTITION (version='${delete_day}.1002');"
fi
# ### END DELETE
