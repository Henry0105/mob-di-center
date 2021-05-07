#!/bin/bash
: '
@owner: menff
@describe: 渠道分析
@projectName: appPkgInfo
@BusinessName: appPkgInfo
@SourceTable: dim_mobdi_mapping.dim_apppkg_name_info_wf,dim_sdk_mapping.dim_app_info_sdk,dim_sdk_mapping.dim_app_category_mapping_par
@TargetTable: dim_sdk_mapping.dim_apppkg_info

@TableRelation:dim_mobdi_mapping.dim_apppkg_name_info_wf,dim_sdk_mapping.dim_app_info_sdk,dim_sdk_mapping.dim_app_category_mapping_par->dim_sdk_mapping.dim_apppkg_info
'

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#mapping
#dim_apppkg_name_info_wf=dim_mobdi_mapping.dim_apppkg_name_info_wf
#dim_app_info_sdk=dim_sdk_mapping.dim_app_info_sdk
#dim_app_category_mapping_par=dim_sdk_mapping.dim_app_category_mapping_par

#out
#dim_apppkg_info=dim_sdk_mapping.dim_apppkg_info

value=`hive -e "show partitions $dim_apppkg_name_info_wf"|tail -n 1`

hive -v -e "
set hive.auto.convert.join=true;   
set hive.exec.dynamic.partition=true; 
set hive.exec.max.dynamic.partitions.pernode=10000;
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
set hive.map.aggr=true;
set hive.auto.convert.join=true;
set hive.mapred.mode=strict; 
set hive.exec.storagehandler.local=false; 
set dfs.socket.timeout=3600000;
set dfs.datanode.socket.write.timeout=3600000; 

insert overwrite table $dim_apppkg_info
select xxx.apppkg as apppkg, 
       coalesce(hh.icon,'') as icon,
       regexp_replace(
         regexp_replace(
           regexp_replace(trim(coalesce(mm.appname, xxx.app_name)),'\\\\\\\\',''),
         '\"',''),
       '(^.*\\\\?.*\\\\?.*$)|(^[^\\\\?]*\\\\?[^\\\\?]+$)|(^\\\\?$)','') name,
       coalesce(mm.cate_id,'other') cate_id,
       coalesce(mm.cate_name,'other') cate_name,
       coalesce(mm.cate_l2_id,'other') cate_l2_id,
       coalesce(mm.cate_l2,'other') cate_l2
from
(
    select *
    from $dim_apppkg_name_info_wf
    where $value
) xxx
left outer join $dim_app_info_sdk hh
on xxx.apppkg=hh.app_id
left outer join
(
    select apppkg, 
           max(appname)appname,
           max(cate_l1_id) cate_id,
           max(cate_l1) cate_name,
           max(cate_l2) cate_l2,
           max(cate_l2_id) cate_l2_id
    from $dim_app_category_mapping_par
    where version='1000'
    group by apppkg
)mm
on xxx.apppkg=mm.apppkg;
"
