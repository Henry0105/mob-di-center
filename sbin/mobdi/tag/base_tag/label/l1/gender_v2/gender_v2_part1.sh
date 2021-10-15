#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")
tmpdb=${dw_mobdi_tmp}

#device_applist_new="dm_mobdi_mapping.device_applist_new"

#app_category_mapping_par="dm_sdk_mapping.app_category_mapping_par"

#gender_app_cate_index1="dm_sdk_mapping.gender_app_cate_index1"

gender_feature_v2_part1="$tmpdb.gender_feature_v2_part1"

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
insert overwrite table $gender_feature_v2_part1 partition(day=$day)
select device,  
max(index1) index1,
max(index2) index2,
max(index3) index3,
max(index4) index4,
max(index5) index5,
max(index6) index6,
max(index7) index7,
max(index8) index8,
max(index9) index9,
max(index10) index10,
max(index11) index11,
max(index12) index12,
max(index13) index13,
max(index14) index14,
max(index15) index15,
max(index16) index16,
max(index17) index17,
max(index18) index18,
max(index19) index19
from(
select device, 
case when index= 55 then cnt else 0 end index1,
case when index= 56 then cnt else 0 end index2,
case when index= 57 then cnt else 0 end index3,
case when index= 58 then cnt else 0 end index4,
case when index= 59 then cnt else 0 end index5,
case when index= 60 then cnt else 0 end index6,
case when index= 61 then cnt else 0 end index7,
case when index= 62 then cnt else 0 end index8,
case when index= 63 then cnt else 0 end index9,
case when index= 64 then cnt else 0 end index10,
case when index= 65 then cnt else 0 end index11,
case when index= 66 then cnt else 0 end index12,
case when index= 67 then cnt else 0 end index13,
case when index= 68 then cnt else 0 end index14,
case when index= 69 then cnt else 0 end index15,
case when index= 70 then cnt else 0 end index16,
case when index= 71 then cnt else 0 end index17,
case when index= 72 then cnt else 0 end index18,
case when index= 73 then cnt else 0 end index19
from
(
select a.device, c.index, count(1) cnt from 
(select device,pkg app_split from $dim_device_applist_new_di where day = '$day') a
join
(select apppkg,cate_l1_id from $dim_app_category_mapping_par where version='1000') b
on a.app_split=b.apppkg
join 
(select cate_l1_id, index from $dim_gender_app_cate_index1) c
on b.cate_l1_id=c.cate_l1_id group by a.device, c.index
) d
) e group by device;
"

hive -e "alter table $gender_feature_v2_part1 drop partition(day<$p7);"