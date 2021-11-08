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

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
appdb=$dm_mobdi_report

#input
device_applist_new=${dim_device_applist_new_di}
income_1001_university_bssid_index="${tmpdb}.income_1001_university_bssid_index"
income_1001_shopping_mall_bssid_index="${tmpdb}.income_1001_shopping_mall_bssid_index"
income_1001_traffic_bssid_index="${tmpdb}.income_1001_traffic_bssid_index"
income_1001_hotel_bssid_index="${tmpdb}.income_1001_hotel_bssid_index"

#output
output_table=${tmpdb}.tmp_score_part4

##-----part4
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
),

tmp_score_part4 as
(
  select device,
         if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
         if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
  from
  (
    select device, index, 1.0 as cnt from $income_1001_university_bssid_index where day='$day'

    union all

    select device, index, 1.0 as cnt from $income_1001_shopping_mall_bssid_index where day='$day'

    union all

    select device, index, 1.0 as cnt
    from $income_1001_traffic_bssid_index
    LATERAL VIEW explode(Array(traffic_bus_index,traffic_subway_index,traffic_airport_index,traffic_train_index)) a as index
    where day='$day'

    union all

    select device, index, 1.0 as cnt
    from $income_1001_hotel_bssid_index
    LATERAL VIEW explode(Array(price_level1_index,price_level2_index,price_level3_index,price_level4_index,price_level5_index,
                               price_level6_index,rank_star1_index,rank_star2_index,rank_star3_index,rank_star4_index,
                               rank_star5_index,score_type1_index,score_type2_index,score_type3_index)) a as index
    where day='$day'
  )a
  group by device
)

insert overwrite table ${output_table} partition(day='${day}')
select t1.device, if(t2.index is null,array(0),t2.index) as index,
       if(t2.cnt is null,array(0.0),cnt) as cnt
from seed t1
left join (select * from tmp_score_part4 where day='${day}') t2
on t1.device=t2.device;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done
