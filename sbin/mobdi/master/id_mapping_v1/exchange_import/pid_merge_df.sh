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

# input
ext_phoneno_v=mobdi_test.ext_phoneno_v

# output
dim_device_pid_merge_df=dim_mobdi_mapping.dim_device_pid_merge_df



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



--step4 合并到正常表
insert overwrite table $dim_device_pid_merge_df partition (day='${insert_day}.1002')
select
   coalesce(b.device,a.device) as device,
   if(size(split( mobdi_array_udf('field', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/'),',')) <= 200 and (pid_abnormal_tm = 'unknown' or pid_abnormal_tm is null),
     mobdi_array_udf('field', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/'),
     a.pid
   ) as pid,

   if(size(split( mobdi_array_udf('field', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/'),',')) <= 200 and (pid_abnormal_tm = 'unknown' or pid_abnormal_tm is null),
     mobdi_array_udf('date', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/'),
     a.pid_tm
   ) as pid_tm,

   if(size(split( mobdi_array_udf('field', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/'),',')) <= 200 and (pid_abnormal_tm = 'unknown' or pid_abnormal_tm is null),
     mobdi_array_udf('date', a.pid, a.pid_ltm, b.pid, b.pid_tm,'max','/hiveDW/dm_mobdi_md/pid_blacklist/'),
     a.pid_ltm
   ) as pid_ltm,

   case
      when (pid_abnormal_tm = 'unknown' or pid_abnormal_tm is null) and size(split( mobdi_array_udf('field', a.pid, a.pid_tm,b.pid, b.pid_tm,'min','/hiveDW/dm_mobdi_md/pid_blacklist/'),',')) > 200 then '$insert_day'
      when pid_abnormal_tm != 'unknown' and pid_abnormal_tm is not null then pid_abnormal_tm
      else 'unknown'
   end as pid_abnormal_tm
from
(
    select *
    from $dim_device_pid_merge_df
    where day = '${insert_day}'
    and device is not null
    and length(device)= 40
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) a
full join
(
    select
      device,
      concat_ws(',',pid_encrypt_array(split(trim(ext_phone),','))) as pid,
      ext_phone_tm as pid_tm
    from $ext_phoneno_v
    where device is not null and ext_phone is not null and length(ext_phone)>0
    and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
) b
on a.device = b.device;
"
