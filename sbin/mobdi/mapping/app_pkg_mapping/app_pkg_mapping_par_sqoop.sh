#!/bin/bash

set -e -x
: '
@owner: wangych || zhtli
@describe: 初步包名渠道清理,dm_sdk_mapping.app_pkg_mapping_par每次更新后，需要将该表插入到标签系统所用的mysql中以供外部使用
@projectName:MobDI
@BusinessName:mapping
@SourceTable:dm_sdk_mapping.pkg_name_mapping,dm_sdk_mapping.pkg_from_channel

@TargetTable:dm_sdk_mapping.app_pkg_mapping

@TableRelation:dm_sdk_mapping.pkg_name_mapping,dm_sdk_mapping.pkg_from_channel->dm_sdk_mapping.app_pkg_mapping|dm_sdk_mapping.app_pkg_mapping->dm_sdk_mapping.app_pkg_mapping_full
'

t1=$1;

if [ $(date -d "$t1" +%w) -eq 2 ] ;then

sqoop export  \
--connect jdbc:mysql://10.90.27.12:3307/mobdi_profile \
--username bigdatarnd --password AC2yH6ETssR1n \
--table t_hive_app_pkg_mapping_par \
--hcatalog-database dm_sdk_mapping \
--hcatalog-table app_pkg_mapping_par \
--hcatalog-partition-keys version \
--hcatalog-partition-values 1000 \
--columns pkg,apppkg,tag,version \
--update-key pkg \
--update-mode allowinsert

fi
