#!/bin/bash

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

#入参
day=$1
new_ver=${day}.1000

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

## 源表
#dwd_dsign_di=dm_mobdi_master.dwd_dsign_di

## 目标表
#dim_mapping_apppkg_appkey_par_df=dim_mobdi_mapping.dim_mapping_apppkg_appkey_par_df
#dim_mapping_apppkg_appkey_par_df_view=dim_mobdi_mapping.dim_mapping_apppkg_appkey_par_df_view

lastPartStr=`hive -e "show partitions $dim_mapping_apppkg_appkey_par_df" | sort | tail -n 1`

if [ -z "$lastPartStr" ]; then
    lastPartStrA=$lastPartStr
fi

if [ -n "$lastPartStr" ]; then
    lastPartStrA=" AND  $lastPartStr"
fi

echo $lastPartStrA

hive -v -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;

insert overwrite table $dim_mapping_apppkg_appkey_par_df partition(version = '${new_ver}')
select coalesce(t1.apppkg,t2.apppkg) as apppkg,
       coalesce(t1.appkey,t2.appkey) as appkey
from
(
    select trim(apppkg) as apppkg,
           trim(appkey) as appkey
    from $dim_mapping_apppkg_appkey_par_df
    where 1=1  ${lastPartStrA}
) t1
full join
(
    select trim(apppkg) as apppkg,
           trim(appkey) as appkey
    from $dwd_dsign_di
    where day = '$day'
    and apppkg is not null
    and trim(apppkg) not in ('','null','NULL')
    and trim(apppkg)=regexp_extract(trim(apppkg),'([a-zA-Z0-9\.\_-]+)',0)
    and appkey is not null
    and trim(appkey) not in ('','null','NULL')
    and trim(appkey)=regexp_extract(appkey,'([0-9a-zA-Z]+)', 0)
    group by trim(apppkg),trim(appkey) ) t2
on t1.apppkg = t2.apppkg and t1.appkey=t2.appkey;
"

hive -v -e "
create or replace view $dim_mapping_apppkg_appkey_par_df_view as
select * from $dim_mapping_apppkg_appkey_par_df
where version='${new_ver}'
;
"

#实现删除过期的分区的功能，只保留最近5个分区
for old_version in `hive -e "show partitions $dim_mapping_apppkg_appkey_par_df " | grep -v '_bak' | sort | head -n -5`
do
    echo "rm $old_version"
    hive -v -e "alter table $dim_mapping_apppkg_appkey_par_df drop if exists partition($old_version)"
done
