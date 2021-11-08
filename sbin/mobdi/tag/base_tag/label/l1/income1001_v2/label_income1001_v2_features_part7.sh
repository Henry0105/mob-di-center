#!/bin/bash
set -x -e
: '
@owner:hugl,luost
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
appdb=$dm_mobdi_report

#input
device_applist_new=${dim_device_applist_new_di}

#input
#label_device_pkg_install_uninstall_year_info_mf

#mapping
#dim_app_pkg_mapping_par="dim_sdk_mapping.dim_app_pkg_mapping_par"
#mapping_income1001_v2_app_index0

#tmp
tmp_income1001_part7_p1=${tmpdb}.tmp_income1001_part7_p1_${day}

#output
output_table_7="${tmpdb}.tmp_income1001_part7"

##-----part_age_unstall_feature-part7
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;
drop table if exists ${tmp_income1001_part7_p1};
create table if not exists ${tmp_income1001_part7_p1} as
with seed as (
  select device
  from $device_applist_new
  where day='$day'
  group by device
),
age_uninstall_1y as (
  select a.device
       , b.index ,1.0 cnt
  from
  (
    select t3.device,coalesce(t4.apppkg, t3.apppkg) apppkg
    from
    (
      select t1.device, coalesce(t2.apppkg, t1.pkg) apppkg from
      (
        select seed.device,pkg from seed left join
        (
          select device, pkg
          from $label_device_pkg_install_uninstall_year_info_mf
          where day=GET_LAST_PARTITION('dm_mobdi_report','label_device_pkg_install_uninstall_year_info_mf','day')
          and refine_final_flag=-1
        )mf
        on seed.device=mf.device
      ) t1
      left join
      (
        select *
        from $dim_app_pkg_mapping_par
        where version='1000'
      ) t2
      on t1.pkg=t2.pkg
      group by t1.device, coalesce(t2.apppkg, t1.pkg)
    )t3
    left join (select * from $dim_app_pkg_mapping_par where version='1000') t4
    on t3.apppkg=t4.pkg
    group by t3.device,coalesce(t4.apppkg, t3.apppkg)
  ) a
  join
  (
    select apppkg, index from $mapping_income1001_v2_app_index0
  ) b
  on a.apppkg=b.apppkg
)
select device,
if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device,index,cnt from age_uninstall_1y t2
)t
group by device;
"

hive -v -e "
set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 10000000;
set hive.groupby.skewindata=true;

with seed as (
  select device from $device_applist_new where day='$day' group by device
)

insert overwrite table ${output_table_7} partition(day='${day}')
select a.device,if(b.index is null,array(0),b.index) as index,
if(b.cnt is null,array(0.0),b.cnt) as cnt
from
seed a
left join ${tmp_income1001_part7_p1} b
on a.device=b.device;
"

## 删除中间临时表
hive -v -e "drop table if exists ${tmp_income1001_part7_p1}"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table_7} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table_7} drop if exists partition($old_version)"
done
