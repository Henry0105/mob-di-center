#!/bin/sh

set -x -e

: '
@owner:yanhw
@describe: part8 在装app的tgi分组特征
@projectName:MOBDI
'
if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1

export HADOOP_USER_NAME=dba

tmpdb=$dm_mobdi_tmp

#optput
output_table_8=${tmpdb}.tmp_car_score_part8
#input
#device_applist_new="dm_mobdi_mapping.device_applist_new"
#mapping
#car_app_tgi_level="dm_sdk_mapping.car_app_tgi_level"
#car_app_tgi_feature_index0="dm_sdk_mapping.car_app_tgi_feature_index0"

#mobdi_analyst_test.zx_0204_car_app_tgi_level => $tmpdb.tmp_car_app_tgi_level
#mobdi_analyst_test.zx_0204_car_app_tgi_feature_index0 => $tmpdb.tmp_car_app_tgi_feature_index0

hive -e "
with seed as
(
  select device,pkg apppkg from $dim_device_applist_new_di where day='$day'
)
,
tmp_car_pre_pre_app_tgi_feature_union2 as(
  select device, concat(index,':',cnt_level) index, cnt
  from (
    select device, concat(tag,':',tgi_level) index,
    case when cnt<=3 then 1 when cnt<=6 then 2 when cnt<=10 then 3 else 4 end cnt_level, 1 cnt
    from (
      select t1.device, t2.tag, tgi_level, count(*) cnt
      from seed t1
      join $dim_car_app_tgi_level t2
      on t1.apppkg=t2.apppkg
      group by t1.device, t2.tag, tgi_level
    ) a
  )t
),
tmp_car_pre_pre_app_tgi_feature_final as (
  select device,
         if(size(collect_list(t2.rk))=0,collect_set(0),collect_list(t2.rk)) as index,
         if(size(collect_list(t1.cnt))=0,collect_set(0.0),collect_list(cast (t1.cnt as double))) as cnt
  from tmp_car_pre_pre_app_tgi_feature_union2 t1
  join $dim_car_app_tgi_feature_index0 t2
  on t1.index=t2.index
  group by t1.device
)
insert overwrite table ${output_table_8} partition(day='${day}')
select t1.device, if(t2.index is null,array(0),t2.index) as index,
       if(t2.cnt is null,array(0.0),cnt) as cnt
from (select device from seed group by device)t1
left join tmp_car_pre_pre_app_tgi_feature_final t2
on t1.device=t2.device;
"