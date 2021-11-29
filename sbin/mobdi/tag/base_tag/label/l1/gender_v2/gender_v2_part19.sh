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

gender_feature_v2_part19="${dm_mobdi_tmp}.gender_feature_v2_part19"

#gender_pkg_topic_wgt="dim_sdk_mapping.dim_gender_pkg_topic_wgt"

hive -e "
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
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $gender_feature_v2_part19 partition(day='$insertday')
select t1.device, avg(topic_0)  topic1, 
avg(topic_1) topic2,
avg(topic_2) topic3,
avg(topic_3) topic4,
avg(topic_4) topic5,
avg(topic_5) topic6,
avg(topic_6) topic7,
avg(topic_7) topic8,
avg(topic_8) topic9,
avg(topic_9) topic10,
avg(topic_10) topic11,
avg(topic_11) topic12,
avg(topic_12) topic13,
avg(topic_13) topic14,
avg(topic_14) topic15,
avg(topic_15) topic16,
avg(topic_16) topic17,
avg(topic_17) topic18,
avg(topic_18) topic19,
avg(topic_19) topic20
from (select device,pkg apppkg from $dim_device_applist_new_di where day = '$day') t1
join $dim_gender_pkg_topic_wgt t2
on t1.apppkg=t2.pkg
group by t1.device;
"