#!/bin/sh

set -x -e

: '
@owner:xiaolm,
@describe:android_full表补数据
@projectName:mobdi
@BusinessName:id_mapping
'

:<<!
@parameters
@day:传入日期参数,为数据插入日期
!

insert_day=$1
pre_1_day=`date -d "$insert_day -1 days" +%Y%m%d`

# input
ext_phoneno_v=mobdi_test.ext_phoneno_v
dim_device_pid_merge_df=dim_mobdi_mapping.dim_device_pid_merge_df

# output
dim_device_id_abnormal_sec_df=dim_mobdi_mapping.dim_device_id_abnormal_sec_df



full_par=`hive -e "show partitions $dim_device_id_abnormal_sec_df" | grep "flag=ieid" |sort -rn |awk -F "/" '{print $1}'| awk -F "=" '{print $2}' | head -n 1`
echo "$full_par"


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

create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';
create temporary function Md5Encrypt as 'com.mob.udf.Md5Encrypt';
create temporary function pidEncrypt as 'com.mob.udf.PidEncrypt';
create temporary function mobdi_array_udf as 'com.youzu.mob.java.udf.MobdiArrayUtilUDF2';
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';


--合并到异常表
insert overwrite table $dim_device_id_abnormal_sec_df partition(day='${insert_day}.1002',flag='pid')
select coalesce(c.device,d.device) as device,
       coalesce(c.pid,d.pid) as id,
       case
         when c.pid_tm is null then d.pid_tm
         when d.pid_tm is null then c.pid_tm
         when c.pid_tm <= d.pid_tm then c.pid_tm
       else d.pid_tm end as id_tm,
       case
         when c.pid_ltm is null then d.pid_ltm
         when d.pid_ltm is null then c.pid_ltm
         when c.pid_ltm <= d.pid_ltm then d.pid_ltm
       else c.pid_ltm end as id_ltm
from
(
  select device,pid,min(pid_tm) as pid_tm,max(pid_tm) as pid_ltm
  from
  (
    select device,col1 as pid,col2 as pid_tm
    from
    (
        select x.device,concat_ws('=',pid,pid_tm) as list
        from
        (
            select
              device,
              concat_ws(',',pid_encrypt_array(split(trim(ext_phone),','))) as pid,
              ext_phone_tm as pid_tm
            from $ext_phoneno_v
            where device is not null and ext_phone is not null and length(ext_phone)>0
            and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
        )x
        left semi join
        (
            select device
            from $dim_device_pid_merge_df
            where day='${insert_day}.1002'
            and pid_abnormal_tm <> 'unknown'
        )y
        on x.device = y.device
    )a
    lateral view explode_tags(list) mytable as col1, col2
  )x
  group by device,pid
)c
full join
(
  select device,id as pid,id_tm as pid_tm,id_ltm as pid_ltm
  from $dim_device_id_abnormal_sec_df
  where day='${pre_1_day}' and flag='pid'
)d
on c.device = d.device and c.pid = d.pid;
"
