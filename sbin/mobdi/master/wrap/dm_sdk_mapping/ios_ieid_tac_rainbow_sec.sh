#!/bin/sh

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
#dim_ios_imei_tac_rainbow=dim_sdk_mapping.dim_ios_imei_tac_rainbow
# 包装后
#dim_ios_ieid_tac_rainbow_sec=dim_sdk_mapping.dim_ios_ieid_tac_rainbow_sec


hive -v -e "
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

insert overwrite table $dim_ios_ieid_tac_rainbow_sec partition(version = '$day')
select if(imei is not null and trim(imei) not in ('','null','NULL','unknown','none','other','未知','na'),concat_ws(',',Md5EncryptArray(split(lower(trim(imei)), ','))),'') as ieid
from $dim_ios_imei_tac_rainbow
where version = '$day';
"