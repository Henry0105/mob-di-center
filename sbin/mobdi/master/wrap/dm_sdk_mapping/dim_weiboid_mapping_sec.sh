#!/bin/bash

set -e -x

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

# 包装前
#dim_weiboid_mapping=dim_sdk_mapping.dim_weiboid_mapping
# 包装后
#dim_weiboid_mapping_sec=dim_sdk_mapping.dim_weiboid_mapping_sec


hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/pid_encrypt.jar;
create temporary function aes_encrypt as 'com.mob.udf.AesEncrypt';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $dim_weiboid_mapping_sec partition (version = '$day')
select if(length(md5p)=32,aes_encrypt(lower(md5p)),null) as pid,
       weibo
from $dim_weiboid_mapping
where version = '$day';
"
