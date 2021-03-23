#!/bin/sh

set -e -x

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
## ==============正式库============

# input
pid_full=archive_id_laws.pid_full

# output
#dim_pid_transform_full_par_sec=dm_mobdi_mapping.dim_pid_transform_full_par_sec

# view
#dim_pid_transform_full_par_secview=dm_mobdi_mapping.dim_pid_transform_full_par_secview

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
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.job.name='pid加密转换表生成';


insert overwrite table $dim_pid_transform_full_par_sec partition (day = '$day',plat = '1')
select concat_ws(',',pid_encrypt_array(split(trim(pid),','))) as pid,
       concat_ws(',',aes_encrypt(shahash(trim(pid)))) as pid_mobfin,
       concat_ws(',',pid_encrypt_array(split(if(length(trim(pid))=11,concat('000860',pid),pid),','))) as pid_zone
from $pid_full
where day = '$day' and plat = 1;

insert overwrite table $dim_pid_transform_full_par_sec partition (day = '$day',plat = '2')
select concat_ws(',',pid_encrypt_array(split(trim(pid),','))) as pid,
       concat_ws(',',aes_encrypt(shahash(trim(pid)))) as pid_mobfin,
       concat_ws(',',pid_encrypt_array(split(if(length(trim(pid))=11,concat('000860',pid),pid),','))) as pid_zone
from $pid_full
where day = '$day' and plat = 2;
"

# 创建/替换视图  pid 转换表
hive -v -e "
create or replace view $dim_pid_transform_full_par_secview
as
select pid, pid_mobfin, pid_zone
from $dim_pid_transform_full_par_sec group by pid, pid_mobfin, pid_zone
"