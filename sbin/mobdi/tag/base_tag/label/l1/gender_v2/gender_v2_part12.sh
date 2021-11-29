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
gender_feature_v2_part2="${tmpdb}.gender_feature_v2_part2"

gender_feature_v2_part12="${tmpdb}.gender_feature_v2_part12"

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
insert overwrite table $gender_feature_v2_part12 partition(day='$insertday')
select t1.device, 
case when index115>0.5 and (index3>0.5 or index57>0.5 ) then 1 else 0 end cate_l2_comb1,
case when index120>0.5 and (index100<=2.5 or index158<=0.5 or index20<=0.5 or index64>0.5) then 1 else 0 end cate_l2_comb2,
case when index130>0.5 and (index188>2.5 or index87>0.5 ) then 1 else 0 end cate_l2_comb3,
case when index162>0.5 and (index152>0.5 or index20>1.5 ) then 1 else 0 end cate_l2_comb4,
case when index87<=0.5 and (index120>0.5 or index115>0.5 ) then 1 else 0 end cate_l2_comb5
from $gender_feature_v2_part2 t1 where day='$insertday';
"