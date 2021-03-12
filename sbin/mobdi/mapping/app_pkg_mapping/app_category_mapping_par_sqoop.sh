#!/bin/bash

set -e -x

: '
@owner: haom
@describe: 根据规则同步 dm_sdk_mapping.app_category_mapping_par 最新分区到 mysql：t_app_category
@projectName:MobDI
@BusinessName:mapping
'

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
#app_category_mapping_par=dm_sdk_mapping.app_category_mapping_par

#output
app_category_mapping_to_mysql=dm_mobdi_tmp.app_category_mapping_to_mysql

app_category_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_sdk_mapping', 'app_category_mapping_par', 'version');
"
last_app_category_mapping_partition=(`hive -e "$app_category_mapping_sql"`)

hive -v -e"
INSERT OVERWRITE TABLE $app_category_mapping_to_mysql
select apppkg as app_pkg, appname as app_name, cate_l1, cate_l2, cate_l1_id, cate_l2_id 
from $app_category_mapping_par
where version='$last_app_category_mapping_partition'
group by apppkg, appname, cate_l1, cate_l2, cate_l1_id, cate_l2_id
"

sqoop export  \
--connect jdbc:mysql://10.21.32.198:3306/mobdi_profile \
--username mobdi_profile --password M8IKE4HFBQ394BsX \
--table t_app_category \
--hcatalog-database dm_mobdi_tmp \
--hcatalog-table app_category_mapping_to_mysql \
--columns app_pkg,app_name,cate_l1,cate_l2,cate_l1_id,cate_l2_id \
--update-key app_pkg \
--update-mode allowinsert

