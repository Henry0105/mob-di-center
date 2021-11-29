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
tmpdb=${dm_mobdi_tmp}
output_table="${tmpdb}.tmp_income1001_part2"
#input
device_applist_new=${dim_device_applist_new_di}
mapping_app_index=${mapping_income1001_v2_app_index0}

## part2 mapping_app_index源数据不同,不能复用
hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
with seed as
(
   select *
     from $device_applist_new
     where day = '$day'
)
insert overwrite table ${output_table} partition(day='${day}')
select x.device
      ,if(y.device is null,array(0), y.index) index
      ,if(y.device is null,array(0.0), y.cnt) cnt
from
(
select device from seed group by device
)x
left join
(
  select device,collect_list(index) index,collect_list(cnt) cnt
  from
  (select a.device
     , b.index ,1.0 cnt
    from
    seed a
    join
    (
      select apppkg,index from $mapping_app_index
    ) b
    on a.pkg=b.apppkg
  )c group by device
)y
on x.device=y.device;
"