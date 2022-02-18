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

gender_feature_v2_part6="${tmpdb}.gender_feature_v2_part6"

#label_l1_applist_refine_cnt_di="rp_mobdi_app.label_l1_applist_refine_cnt_di"

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
insert overwrite table $gender_feature_v2_part6 partition(day='$insertday')
select t1.device, 
t1.tgi_male_high/(cast (tot_install_apps as float)) tgi_male_high,
t1.tgi_male/(cast (tot_install_apps as float)) tgi_male,
t1.tgi_normal/(cast (tot_install_apps as float)) tgi_normal,
t1.tgi_female/(cast (tot_install_apps as float)) tgi_female,
t1.tgi_female_high/(cast (tot_install_apps as float)) tgi_female_high
from $gender_feature_v2_part5 t1 
join (select device,cnt tot_install_apps from $label_l1_applist_refine_cnt_di where day = '$day') t2
on t1.device=t2.device
where t1.day='$insertday';
"
