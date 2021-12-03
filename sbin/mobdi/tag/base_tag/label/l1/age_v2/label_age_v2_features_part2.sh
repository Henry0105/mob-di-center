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
#mapping_age_app_index

#output
output_table=${tmpdb}.tmp_score_part2

hive -v -e "
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
    from seed a
    join
    (
      select apppkg, index_after_chi index from $mapping_age_app_index where version='1000'
    ) b
    on a.pkg=b.apppkg
  )c group by device
)y
on x.device=y.device;
"