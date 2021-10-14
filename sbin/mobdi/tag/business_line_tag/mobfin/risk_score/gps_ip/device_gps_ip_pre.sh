#!/bin/bash
: '
@owner:luost
@describe:gps,ip一致性评分预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1
p30day=`date -d "$day -30 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di

#输出表
tmp_anticheat_device_gps_ip_pre=$dw_mobdi_tmp.tmp_anticheat_device_gps_ip_pre


hive -v -e "
SET hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=200000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coord_convertor as 'com.youzu.mob.java.udf.CoordConvertor';

insert overwrite table $tmp_anticheat_device_gps_ip_pre partition (day = '$day')
select muid as device,
       split(coord_convertor(latitude,longitude,'wsg84','bd09'), ',')[0] as lat, 
       split(coord_convertor(latitude,longitude,'wsg84','bd09'), ',')[1] as lon, 
       clientip,
       day as connect_day
from $dwd_location_info_sec_di
where day > '$p30day' 
and day <= '$day'
and abs(latitude) <= 90 
and abs(longitude) <= 180 
and (latitude <> 0 or longitude <> 0) 
and clientip is not null 
and clientip <> ''
and latitude is not null 
and longitude is not null
and serdatetime is not null 
and clienttime is not null
and (serdatetime - clienttime) <= 60000
and (latitude - round(latitude, 1))*10 <> 0.0 
and (longitude - round(longitude, 1))*10 <> 0.0;
"