#!/bin/sh

set -e -x

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

# 包装前
#dim_phone_bank_blacklist=dim_sdk_mapping.dim_phone_bank_blacklist
# 包装后
#dim_pid_bank_blacklist_sec=dim_sdk_mapping.dim_pid_bank_blacklist_sec



hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx4096m';
SET mapreduce.child.map.java.opts='-Xmx4096m';

insert overwrite table $dim_pid_bank_blacklist_sec partition (day='${day}')
select
	if(phone is not null and trim(phone) not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',pid_encrypt_array(split(trim(phone),','))),'') as pid,
	if(phone_clean is not null and trim(phone_clean) not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',pid_encrypt_array(split(trim(phone_clean),','))),'') as pid_clean,
	flag
from $dim_phone_bank_blacklist where day='${day}';
"