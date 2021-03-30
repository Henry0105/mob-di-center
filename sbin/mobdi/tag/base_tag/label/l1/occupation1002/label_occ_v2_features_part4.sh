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

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1
tmpdb=${dw_mobdi_md}
appdb="rp_mobdi_report"
#input
device_applist_new=${dim_device_applist_new_di}
mapping_app_index="dm_sdk_mapping.mapping_app_income_index"

HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${tmpdb}.tmp_occ1002_predict_part4;
create table ${tmpdb}.tmp_occ1002_predict_part4 stored as orc as
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device, index, 1.0 as cnt from dw_mobdi_md.income_1001_university_bssid_index where day='$day'

  union all

  select device, index, 1.0 as cnt from dw_mobdi_md.income_1001_shopping_mall_bssid_index where day='$day'

  union all

  select device, index, 1.0 as cnt
  from dw_mobdi_md.income_1001_traffic_bssid_index
  LATERAL VIEW explode(Array(traffic_bus_index,traffic_subway_index,traffic_airport_index,traffic_train_index)) a as index
  where day='$day'

  union all

  select device, index, 1.0 as cnt
  from dw_mobdi_md.income_1001_hotel_bssid_index
  LATERAL VIEW explode(Array(price_level1_index,price_level2_index,price_level3_index,price_level4_index,price_level5_index,
                             price_level6_index,rank_star1_index,rank_star2_index,rank_star3_index,rank_star4_index,
                             rank_star5_index,score_type1_index,score_type2_index,score_type3_index)) a as index
  where day='$day'
)a
group by device
"