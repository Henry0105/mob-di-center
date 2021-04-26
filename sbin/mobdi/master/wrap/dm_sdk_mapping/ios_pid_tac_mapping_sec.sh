#!/bin/bash

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
#dim_ios_phone_tac_mapping=dim_sdk_mapping.dim_ios_phone_tac_mapping
# 包装后
#dim_ios_pid_tac_mapping_sec=dim_sdk_mapping.dim_ios_pid_tac_mapping_sec


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function pid_encrypt_array as 'com.mob.udf.PidEncryptArray';
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/etl_udf-1.1.2.jar;
create temporary function Md5EncryptArray as 'com.mob.udf.Md5EncryptArray';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $dim_ios_pid_tac_mapping_sec partition (day = '$day')
select if(phone is not null and trim(phone) not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',pid_encrypt_array(split(trim(phone),','))),'') as pid,
       if(imei is not null and trim(imei) not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',Md5EncryptArray(split(lower(trim(imei)), ','))),'') as ieid,
       tac,
       brand,
       model,
       public_date,
       public_price,
       latest_price_date,
       price,
       model_level,
       profile
from $dim_ios_phone_tac_mapping
where day = '$day';
"