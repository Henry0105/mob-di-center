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

#mapping
#mapping_age_app_index0

#output
output_table_8=${tmpdb}.tmp_score_part8

##-----part_age_unstall_feature-part7
hive -v -e "
with seed as (
  select device,pkg from $device_applist_new where day='$day'
),

age_pre_app_tgi_feature_union as (
select t3.device, concat(t4.tag,':',tgi_level) index, count(*) cnt
from
(
    select t1.device, t2.apppkg
    from
    (
      select a.device
           , b.index ,1.0 cnt
      from seed a
      join
      (
      select apppkg, index from $mapping_age_app_index0 where version='1000'
      ) b
      on a.pkg=b.apppkg
    ) t1
    join
    (
    select apppkg, index from $mapping_age_app_index0 where version='1000'
    ) t2
    on t1.index=t2.index
)t3
join $mapping_age_app_tgi_level t4
on t3.apppkg=t4.apppkg
where t4.apppkg not in ('com.xwtec.sd.mobileclient','com.hanweb.android.sdzwfw.activity','com.inspur.vista.labor','com.android.clock.sd','com.qdccb.bank','com.sdhs.easy.high.road')
group by t3.device, t4.tag, tgi_level
),

age_pre_app_tgi_feature_final as
(
  select device,
  if(index is null,array(0),index) as index,
       if(cnt is null,array(0.0),cnt) as cnt
  from
  (
  select device,
         if(size(collect_list(t2.rk))=0,collect_set(0),collect_list(t2.rk)) as index,
         if(size(collect_list(t1.cnt))=0,collect_set(0.0),collect_list(cast (t1.cnt as double))) as cnt
  from age_pre_app_tgi_feature_union t1
  join $mapping_age_app_tgi_feature_index0 t2
  on t1.index=t2.index
  group by t1.device
  )t
)

insert overwrite table ${output_table_8} partition(day='${day}')
select device,index,cnt from age_pre_app_tgi_feature_final;
"
