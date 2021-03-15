#!/bin/bash

set -e -x

if [ -z "$1" ]; then
  exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#日更，从20200301分区开始刷数据
## 源表
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di

## 目标表
#dim_ad_apppkg_appkey_par_di=dm_mobdi_mapping.dim_mapping_ad_apppkg_appkey_par_di

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

insert overwrite table $dim_ad_apppkg_appkey_par_di partition (day='$day')
select apppkg, appkey, plat, count(*) as device_cnt
from 
(
  select device, apppkg, appkey, plat
  from 
  (
    select muid as device, apppkg, appkey, plat
    from $dwd_pv_sec_di
    where day='$day'
    and apppkg = regexp_extract(apppkg,'([a-zA-Z0-9\.\_-]+)',0) 
    and appkey rlike '^[0-9a-fA-F]{1,40}$' 
    and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
    and trim(deviceid)!='0000000000000000000000000000000000000000'
    and plat in (1, 2)
    
    union all
    
    select muid as device, apppkg, appkey, plat
    from $dwd_log_run_new_di
    where day = '$day'
    and apppkg = regexp_extract(apppkg,'([a-zA-Z0-9\.\_-]+)',0) 
    and appkey rlike '^[0-9a-fA-F]{1,40}$' 
    and trim(lower(muid)) rlike '^[a-f0-9]{40}$'
    and plat in (1, 2)
  ) as a 
  group by device, apppkg, appkey, plat
) as b 
group by apppkg, appkey, plat;
"
