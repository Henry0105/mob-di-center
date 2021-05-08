#!/usr/bin/env bash
: '
@owner: guanyt
@describe: 同步显示库的mysql配置到hive，目前待配置调度和自动化
'

sqoop import --connect jdbc:mysql://10.21.32.198 --username mobdi_profile --password M8IKE4HFBQ394BsX \
--query "select mob_id,profile_id,profile_name,profile_version_id,profile_database,profile_table,profile_column,os from mobdi_profile.v_id_mapping where \$CONDITIONS " \
--hive-import \
--hive-overwrite \
--hive-database test \
--hive-table guanyt_profile_id_mapping \
--target-dir /user/hive/warehouse/test.db/guanyt_profile_id_mapping \
--delete-target-dir \
--split-by profile_id

insert overwrite table dim_sdk_mapping.dim_mobid_profilename_mapping
select mob_id,profile_id,profile_name,profile_version_id,profile_database,profile_table,profile_column,os
from test.guanyt_profile_id_mapping