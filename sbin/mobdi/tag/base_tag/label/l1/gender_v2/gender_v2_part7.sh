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

tmpdb=$dm_mobdi_tmp
gender_feature_v2_part5="${tmpdb}.gender_feature_v2_part5"

gender_feature_v2_part7="${tmpdb}.gender_feature_v2_part7"

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
insert overwrite table $gender_feature_v2_part7 partition(day='$insertday')
select device,
case when tgi_male_high+tgi_male=0 and tgi_female+tgi_female_high>0 then 0 
when tgi_female+tgi_female_high=0 and tgi_male_high+tgi_male>0 then 2 
when tgi_female+tgi_female_high=0 and tgi_male_high+tgi_male>0 then -1 
else (tgi_male_high+tgi_male)/(cast ((tgi_female+tgi_female_high) as float)) end tgi_male_female 
from $gender_feature_v2_part5 where day='$insertday';
"
