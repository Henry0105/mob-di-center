#!/bin/bash
set -x -e
: '
@owner:guanyt
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
tmpdb=$dm_mobdi_tmp
appdb=$dm_mobdi_report
#input
device_applist_new=${dim_device_applist_new_di}

#mapping
#mapping_age_app_index0

#tmp
#output
output_table_v3=${tmpdb}.tmp_score_part2_v3

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
with seed as (
  select device,pkg from $device_applist_new where day='$day'
)
insert overwrite table ${tmpdb}.tmp_score_part2_v3 partition(day='${day}')
select x.*
from
(
  select device,
         if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
         if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
  from (
     select a.device
           , b.index ,1.0 cnt
      from  seed  a
      left join
      (
      select apppkg, index from $mapping_age_app_index0 where version='1000'
      ) b
      on a.pkg=b.apppkg
  )t
  where index not in (67,473,378,238,783,379)
  group by device
)x；
"
