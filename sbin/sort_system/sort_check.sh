#!/bin/bash

set -e -x

: '
@owner:huarong
@describe: sort_app_v2.sh 执行完之后，会生成 dw_mobdi_md.sorted_pkg, 取出dw_mobdi_md.sorted_pkg表中新增的数据和数据库中分类表 app_catetory join,
           数据写入hive分区表,另外保存一份到mysql,方便分拣师进行手工分拣
@projectName:SortingSystem
'

day=$1
sorted_pkg=dw_mobdi_md.sorted_pkg

mysqlInfo_category='{"userName":"root","pwd":"mobtech2019java","dbName":"sorting_system","host":"10.89.120.12","port":3310,"tableName":"app_category"}'
mysqlInfo_tmp='{"userName":"root","pwd":"mobtech2019java","dbName":"sorting_system","host":"10.89.120.12","port":3310,"tableName":"app_name_tmp"}'

selectSql="
select
  pkg, apppkg, appname, type
from
(
  select
    a.pkg as pkg,
    a.apppkg as apppkg,
    a.appname as appname,
    a.type as type
  from
  ( select pkg, apppkg, appname, type from $sorted_pkg where type='1') as a
  LEFT JOIN
  ( select pkg from app_category where note <> 'unknown' group by pkg ) b
  on a.pkg=b.pkg
  where b.pkg is null

  union all

  select pkg, apppkg, appname, type from $sorted_pkg where type='2'
) t1
group by pkg, apppkg, appname, type
"

hive_db=dm_mobdi_tmp
hive_table=app_top_pickup_pkg


spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.sortsystem.SortSystemCheck \
--driver-memory 6G \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.dynamicAllocation.executorIdleTimeout=20s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--executor-memory 12G --executor-cores 4 \
--name "SortSystemCheck" \
--conf spark.sql.shuffle.partitions=100 \
--conf spark.sql.autoBroadcastJoinThreshold=519715200 \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$mysqlInfo_category" "$mysqlInfo_tmp" "$selectSql" "$hive_db" "$hive_table" "$day"

