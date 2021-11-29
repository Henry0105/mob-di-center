#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")
insertday=${day}_muid
#device_applist_new="dm_mobdi_mapping.device_applist_new"

gender_feature_v2_part8="${dm_mobdi_tmp}.gender_feature_v2_part8"

#gender_app_tgi_level_5="dm_sdk_mapping.gender_app_tgi_level_5"

hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table $gender_feature_v2_part8 partition(day='$insertday')
select t1.device, sum(tgi) sum_tgi, count(*) cnt, avg(tgi) avg_tgi
from (select device,pkg apppkg from $dim_device_applist_new_di where day = '$day') t1
join $dim_gender_app_tgi_level_5 t2
on t1.apppkg=t2.apppkg
group by t1.device;
"