#!/bin/sh
set -x -e
export LANG=en_US.UTF-8

: '
@owner:guanyt
@describe:为训练模型准备好各种特征和数据
@projectName:model
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <date>"
    exit 1
fi

day=$1
source /home/dba/mobdi_center/conf/hive-env.sh

## input
label_device_applist_cnt=$label_l1_applist_refine_cnt_di
label_citylevel_df=$label_l2_citylevel_df
dwd_device_info_df=$dwd_device_info_df
label_house_price_mf=$label_l1_house_price_mf
label_diff_month_mf=$label_l1_diff_month_df
label_apppkg_category_index=$label_l1_apppkg_category_index

sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_report', 'label_l1_house_price_mf', 'day');
drop temporary function GET_LAST_PARTITION;
"
lastPartition=(`hive -e "$sql"`)
##tmp table
label_merge_all=$dw_mobdi_md.model_merge_all_features
## output
transfered_feature_table=$dw_mobdi_tmp.model_transfered_features

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
--201908改版后city_level取值从常驻城市改为device_ip_info，所以表中的country,province,city用不着了，所以全设为null
insert overwrite table $label_merge_all partition (day = ${day})
select a1.device,null as country,null as province,null as city,
       a3.factory, a3.sysver, a3.public_date, a5.diff_month, a3.price,
       a2.city_level, a2.city_level_1001, a1.tot_install_apps, a4.house_price
from
(
  select device,cnt as tot_install_apps
  from $label_device_applist_cnt
  where day = ${day}
) as a1
left join
(
  select device,city_level,city_level_1001
  from $label_citylevel_df
  where day=$day
) as a2 on a1.device = a2.device
left join
(
  select device, upper(factory_clean_subcompany) as factory, sysver,
         case
           when trim(public_date) in ('', 'null', 'NULL') or public_date is null then 'unknown'
           else public_date
         end as public_date,
         case
           when price is null or length(trim(price)) = 0 then 'unknown'
           else price
         end as price
  from $dwd_device_info_df
  where version = '$day.1000'
  and plat = '1'
) a3 on a1.device = a3.device
left join
(
  select device,house_price
  from $label_house_price_mf
  where day='${lastPartition}'
) a4 on a1.device = a4.device
left join
(
  select device,diff_month
  from $label_diff_month_mf
  where day=$day
) a5 on a1.device = a5.device ;
"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.parallel=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $transfered_feature_table partition(day=$day)
select device, index, cnt
from
(
  select device,
         case
           when city_level = -1 then 0
           when city_level = 1 then 1
           when city_level = 2 then 2
           when city_level = 3 then 3
           when city_level = 4 then 4
           when city_level = 5 then 5
           else 6
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when factory in ('HUAWEI','HONOR') then 8
           when factory = 'OPPO' then 9
           when factory = 'VIVO' then 10
           when factory = 'XIAOMI' then 11
           when factory = 'SAMSUNG' then 12
           when factory = 'MEIZU' then 13
           when factory = 'GIONEE' then 14
           else 15
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
    when split(sysver, '\\\\.')[0] >= 8 then 16
    when split(sysver, '\\\\.')[0] = 7 then 17
    when split(sysver, '\\\\.')[0] = 6 then 18
    when split(sysver, '\\\\.')[0] = 5 then 19
    when split(sysver, '\\\\.')[0] <= 4 then 20
    when sysver = 'unknown' or sysver = 'API' then 21
           else 22
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when diff_month < 12 then 23
           when diff_month >= 12 and diff_month < 24 then 24
           when diff_month >= 24 and diff_month < 30 then 25
           when diff_month >= 30 then 26
           else 27
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when price > 0 and price < 1000 then 33
           when price >= 1000 and price < 1499 then 34
           when price >= 1499 and price < 2399 then 35
           when price >= 2399 then 36
           else 37
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when tot_install_apps <= 7 then 28
           when tot_install_apps > 7 and tot_install_apps <= 15 then 29
           when tot_install_apps > 15 and tot_install_apps <= 32 then 30
           when tot_install_apps > 32 and tot_install_apps <= 53 then 31
           else 32
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}
  and tot_install_apps >= 5 and tot_install_apps <= 200

  union all

  select device,
         case
           when house_price >= 0 and house_price < 8000 then 38
           when house_price >= 8000 and house_price < 12000 then 39
           when house_price >= 12000 and house_price < 22000 then 40
           when house_price >= 22000 and house_price < 40000 then 41
           when house_price >= 40000 and house_price < 50000 then 42
           else 43
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}
  and house_price is not null

  union all

  select device,index,cnt
  from $label_apppkg_category_index
  where day = ${day}
  and version = '1003'
) as aa;
"
