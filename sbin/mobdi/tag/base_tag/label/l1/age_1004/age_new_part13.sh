#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

day=$1
day_before_one_month=$(date -d "${day} -1 month" "+%Y%m%d")
source /home/dba/mobdi_center/conf/hive-env.sh
insertday=${day}_muid
#label_device_pkg_install_uninstall_year_info_mf="rp_mobdi_app.label_device_pkg_install_uninstall_year_info_mf"

age_new_topic_wgt="${dm_mobdi_tmp}.age_new_topic_wgt"
#dim_pkg_topic_wgt=dim_mobdi_mapping.dim_pkg_topic_wgt
#pkg_topic_wgt="dm_mobdi_mapping.pkg_topic_wgt"

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
insert overwrite table  $age_new_topic_wgt partition (day='$insertday')
select device,
avg( topic_0) as topic_0,
avg( topic_1)as topic_1,
avg( topic_2)as topic_2,
avg( topic_3)as topic_3,
avg( topic_4)as topic_4,
avg( topic_5)as topic_5,
avg( topic_6)as topic_6,
avg( topic_7)as topic_7,
avg( topic_8)as topic_8,
avg( topic_9)as topic_9,
avg( topic_10)as topic_10,
avg( topic_11)as topic_11,
avg( topic_12)as topic_12,
avg( topic_13)as topic_13,
avg( topic_14)as topic_14,
avg( topic_15)as topic_15,
avg( topic_16)as topic_16,
avg( topic_17)as topic_17,
avg( topic_18)as topic_18,
avg( topic_19)as topic_19
from $dim_pkg_topic_wgt a
inner join 
(
    select device,pkg
    from $label_device_pkg_install_uninstall_year_info_mf
    where day=$day
    and update_day between $day_before_one_month and $day
)b
on a.pkg=b.pkg
group by device;
"