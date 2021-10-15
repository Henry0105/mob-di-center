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
tmpdb="$dw_mobdi_md"
#input
#device_applist_new="dm_mobdi_mapping.device_applist_new"

#mapping mobdi_analyst_test.zx_0204_car_app_index -> dm_sdk_mapping.mapping_car_app_index
#dim_mapping_car_app_index=dim_sdk_mapping.dim_mapping_car_app_index
#mapping_car_app_index="dm_sdk_mapping.mapping_car_app_index"

output_table=${tmpdb}.tmp_car_score_part2

:<<!
hive -v -e "
--catel1,55-73
--放在label_apppkg_feature_category_index.sh里面
--9 cate l2 --74-287
"
!
:<<!
hive -v -e "
CREATE TABLE mobdi_test.label_age_app_unstall_1y(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc ;


CREATE TABLE mobdi_test.label_age_pre_app_tgi_feature(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc ;

CREATE TABLE mobdi_test.label_age_pre_app_tgi_feature(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc ;

CREATE TABLE mobdi_test.label_age_applist_part2_beforechi(
  device string,
  index array<int>,
  cnt array<double>,
  tag int)
partitioned by (day string)
stored as orc ;
"
!

##-----part2
{
hive -v -e "
with seed as
(
  select * from $dim_device_applist_new_di where day = '$day'
)
insert overwrite table ${output_table} partition(day='${day}')
select device,
         if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
         if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
  from (
     select a.device, b.index ,if(b.index is not null,1.0,null) cnt
      from seed a
      left join
      (
        select apppkg, index_after_chi index from $dim_mapping_car_app_index
      ) b
      on a.pkg=b.apppkg
  )t
  group by device
"
}

##-----part_age_applist_part2_v3
{
#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done

}
