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
#device_applist_new="dm_mobdi_mapping.device_applist_new"

gender_feature_v2_part1="${tmpdb}.gender_feature_v2_part1"

gender_feature_v2_part3="${tmpdb}.gender_feature_v2_part3"


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
insert overwrite table $gender_feature_v2_part3 partition(day='$insertday')
select t1.device,
(index1)/(cast (t2.tot_install_apps as float)) index1,
(index2)/(cast (t2.tot_install_apps as float)) index2,
(index3)/(cast (t2.tot_install_apps as float)) index3,
(index4)/(cast (t2.tot_install_apps as float)) index4,
(index5)/(cast (t2.tot_install_apps as float)) index5,
(index6)/(cast (t2.tot_install_apps as float)) index6,
(index7)/(cast (t2.tot_install_apps as float)) index7,
(index8)/(cast (t2.tot_install_apps as float)) index8,
(index9)/(cast (t2.tot_install_apps as float)) index9,
(index10)/(cast (t2.tot_install_apps as float)) index10,
(index11)/(cast (t2.tot_install_apps as float)) index11,
(index12)/(cast (t2.tot_install_apps as float)) index12,
(index13)/(cast (t2.tot_install_apps as float)) index13,
(index14)/(cast (t2.tot_install_apps as float)) index14,
(index15)/(cast (t2.tot_install_apps as float)) index15,
(index16)/(cast (t2.tot_install_apps as float)) index16,
(index17)/(cast (t2.tot_install_apps as float)) index17,
(index18)/(cast (t2.tot_install_apps as float)) index18,
(index19)/(cast (t2.tot_install_apps as float)) index19
from $gender_feature_v2_part1 t1 
join (select device,count(pkg) tot_install_apps from $dim_device_applist_new_di where day = '$day' group by device) t2
on t1.device=t2.device
where t1.day='$insertday';
"

#hive -e "alter table $gender_feature_v2_part3 drop partition(day<$p7);"
