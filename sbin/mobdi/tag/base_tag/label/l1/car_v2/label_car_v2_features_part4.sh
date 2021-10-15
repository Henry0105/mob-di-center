#!/bin/sh

set -x -e

: '
@owner:hugl
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb="$dw_mobdi_md"

output_table="${tmpdb}.tmp_car_score_part4"
#input
tmp_score_part4=${tmpdb}.tmp_score_part4
#device_applist_new="dm_mobdi_mapping.device_applist_new"

## part4 复用年龄标签part4 + 添加无此特征数据
HADOOP_USER_NAME=dba hive -e"
with seed as
(
  select device
  from $dim_device_applist_new_di
  where day = '$day'
  group by device
)
insert overwrite table ${output_table} partition(day='${day}')
select t1.device, if(t2.index is null,array(0),t2.index) as index,
       if(t2.cnt is null,array(0.0),cnt) as cnt
from seed t1
left join (select * from $tmp_score_part4 where day='${day}') t2
on t1.device=t2.device;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done