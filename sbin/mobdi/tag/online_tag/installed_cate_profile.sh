#!/bin/bash
: '
@owner: menff
@describe: 
@projectName: MobDI
@BusinessName: category_tag
@SourceTable: dm_sdk_mapping.app_category_mapping_par,dm_mobdi_mapping.device_applist_new,dw_mobdi_md.cleaned_app_category_mapping
@TargetTable: dw_mobdi_md.cleaned_app_category_mapping,rp_mobdi_app.installed_cate_profile
@TableRelation: dm_sdk_mapping.app_category_mapping_par,dm_mobdi_mapping.device_applist_new->rp_mobdi_app.installed_cate_profile
'

: '
1. 将dm_sdk_mapping.app_category_mapping_par清理成cleaned_app_category_mapping
2. 将base表中的pkg清理成apppkg
3. 根据apppkg去join cleaned_app_category_mapping,将一级分类和二级分类分成2行
'

set -x -e
export LANG=zh_CN.UTF-8

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

day="$1"

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#databases
tmp=dm_mobdi_tmp

#input
#dim_device_applist_new_di=dim_mobdi_mapping.dim_device_applist_new_di

#mapping
#app_category_mapping_par=dim_sdk_mapping.app_category_mapping_par

#tmp
cleaned_app_category_mapping=${tmp}.cleaned_app_category_mapping

#output
#installed_cate_profile=dm_mobdi_report.installed_cate_profile

hive -v -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.map.aggr=true;
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.exec.parallel=true;

insert overwrite table $cleaned_app_category_mapping
select apppkg,cate_l1_id,cate_l2_id
from $app_category_mapping_par
where version='1000'
group by apppkg,cate_l1_id,cate_l2_id;

insert overwrite table $installed_cate_profile partition (day='$day')
select device, cate_id, count(1) as cnt
from (
    select stack(2, 
        device, d.apppkg, cat.cate_l1_id, 
        device, d.apppkg, cat.cate_l2_id) as (device, apppkg, cate_id)
    from
    (
        select device, pkg as apppkg
        from $dim_device_applist_new_di
        where day=$day
        and processtime='$day'
        group by device, pkg
    ) as d
    join $cleaned_app_category_mapping as cat
    on d.apppkg = cat.apppkg
) as e
group by device, cate_id;
"
