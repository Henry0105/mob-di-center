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
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp

#input
#dim_device_applist_new_di

#mapping mobdi_analyst_test.zx_0204_car_app_index -> dm_sdk_mapping.mapping_car_app_index
#dim_mapping_car_app_index=dim_sdk_mapping.dim_mapping_car_app_index
#mapping_car_app_index="dm_sdk_mapping.mapping_car_app_index"

output_table=${tmpdb}.tmp_car_score_part2


##-----part2
hive -v -e "
with seed as
(
  select * from $dim_device_applist_new_di where day = '$day'
)
insert overwrite table ${output_table} partition(day='${day}')
select device,
         if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
         if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
  from
  (
      select a.device, b.index ,if(b.index is not null,1.0,null) cnt
      from seed a
      left join
      (
        select apppkg, index_after_chi index
        from $dim_mapping_car_app_index
      ) b
      on a.pkg=b.apppkg
  )t
  group by device;
"
