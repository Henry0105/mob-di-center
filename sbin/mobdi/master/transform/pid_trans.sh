#!/bin/sh

set -e -x

day=$1
#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
# input
#pid_full=${archive_id_laws}.pid_full

# output
#dim_pid_transform_full_par_sec=dim_mobdi_mapping.dim_pid_transform_full_par_sec

# view
#dim_pid_transform_full_par_secview=dim_mobdi_mapping.dim_pid_transform_full_par_secview


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function shahash as 'com.youzu.mob.java.udf.SHAHashing';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function aes_encrypt as 'com.mob.udf.AesEncrypt';
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';

set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.name='pid加密转换表生成';

insert overwrite table $dim_pid_transform_full_par_sec partition (day = '$day')
select concat_ws(',',pid_encrypt_array(split(trim(pid),','))) as pid,
       concat_ws(',',aes_encrypt(shahash(trim(pid)))) as pid_mobfin,
       concat_ws(',',pid_encrypt_array(split(if(length(trim(pid))=11,concat('000860',pid),pid),','))) as pid_zone
from
(
    select pid_cleaned as pid
    from $pid_full
    where pid_cleaned <> ''
    and pid_cleaned is not null
    and day = '$day'
    and (trim(pid_cleaned) rlike '^[0-9]{11}$' or trim(pid_cleaned) rlike '^[0-9]{17}$')
    group by pid_cleaned
)a;
"

# 创建/替换视图  pid 转换表
hive -v -e "
create or replace view $dim_pid_transform_full_par_secview
as
select pid, pid_mobfin, pid_zone
from $dim_pid_transform_full_par_sec
group by pid, pid_mobfin, pid_zone
"