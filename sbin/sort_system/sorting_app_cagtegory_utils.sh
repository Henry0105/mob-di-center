#!/bin/bash

set -e -x

: '
@owner:zhtli
@describe: 实现分拣系统中，将app标签分类从MySQL导入hive中去，并check数据，如果数据无效，则发邮件
@projectName:SortingSystem
'

source /home/dba/mobdi_center/conf/hive-env.sh

#dim_app_category_mapping_par=dim_sdk_mapping.dim_app_category_mapping_par
mapping_db=${dim_app_category_mapping_par%.*}
mapping_tb=${dim_app_category_mapping_par#*.}

result=`mysql -h10.21.33.28 -uroot -p'mobtech2019java' -P3306 -e "select max(time) from sorting_system.app_category" -sN`
targetPar=1000.$result

parSql="
add jar hdfs://ShareSdkHadoop/user/haom/udf/original-hive_udf-1.0.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$mapping_db','$mapping_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
lastPartition=$(hive -e "$parSql" -SN)

if [ "$lastPartition" \< "$targetPar" ];then
  echo "更新mapping表数据"
else
  exit 0
fi

mysqlInfoStr='{"userName":"root","pwd":"mobtech2019java","dbName":"sorting_system","host":"10.21.33.28","port":3306,"tableName":"app_category"}'
mailList="DIMonitor@mob.com;yqzhou@mob.com"

#其中mysql_table_info_tmp表是spark临时表，用于存储mysql拉取过来的数据
#sql结果存入spark临时表test_info_tmp中
insertTestTableSql="
select pkg, apppkg, appname, cate_l1, cate_l2, cate_l1_id, cate_l2_id
from
(
  select a.pkg, a.apppkg, a.appname, a.cate_l1, a.cate_l2, a.cate_l3, c.cate_l1_id, b.cate_l2_id, a.note, a.time
  from mysql_table_info_tmp a
  left join
  (
    select trim(cate_l2) as cate_l2,cate_l2_id
    from dm_sdk_mapping.cate_id_mapping_par
    where version='1000'
    group by trim(cate_l2),cate_l2_id
  ) b on trim(a.cate_l2)=trim(b.cate_l2)
  left join
  (
    select trim(cate_l1) as cate_l1,cate_l1_id
    from dm_sdk_mapping.cate_id_mapping_par
    where version='1000'
    group by trim(cate_l1),cate_l1_id
  ) c on trim(a.cate_l1)=trim(c.cate_l1)
  where a.note != 'unknown'
  and a.pkg != ''
  group by a.pkg, a.apppkg, a.appname, a.cate_l1, a.cate_l2, a.cate_l3, c.cate_l1_id, b.cate_l2_id, a.note, a.time
) d
"

checkDataSqls="
--校验cate_l1、cate_l2是否有异常
select *
from test_info_tmp
where cate_l1_id is null or cate_l2_id is null;

--校验同一个apppkg是否出现了多次不同分类
select *
from test_info_tmp c
where c.apppkg in (
  select apppkg
  from
  (
    select apppkg, count(cate_l1) as cnt_cate1, count(cate_l2) as cnt_cate2
    from
    (
      select apppkg, cate_l1, cate_l2
      from test_info_tmp
      group by apppkg, cate_l1, cate_l2
    ) a
    group by apppkg
  )b
  where b.cnt_cate1>=2 or b.cnt_cate2>=2
);

--校验pkg是否重复
select pkg,count(1) cnt
from test_info_tmp
group by pkg having cnt>1;

--校验cate_l1和cate_l2是否属于同一体系中
select pkg,apppkg,appname,cate_l1_id,cate_l2_id
from test_info_tmp
where cate_l1_id != substr(cate_l2_id,1,4)
"

bakAndInsertSql="
insert overwrite table $dim_app_category_mapping_par partition(version=$targetPar)
select trim(regexp_replace(pkg, '\\n|\\r', '')) as pkg,
       trim(regexp_replace(apppkg, '\\n|\\r', '')) as apppkg,
       trim(regexp_replace(appname, '\\n|\\r', '')) as appname,
       trim(regexp_replace(cate_l1, '\\n|\\r', '')) as cate_l1,
       trim(regexp_replace(cate_l2, '\\n|\\r', '')) as cate_l2,
       trim(regexp_replace(cate_l1_id, '\\n|\\r', '')) as cate_l1_id,
       trim(regexp_replace(cate_l2_id, '\\n|\\r', '')) as cate_l2_id
from
(
  select a.pkg, a.apppkg, a.appname, a.cate_l1, a.cate_l2, a.cate_l1_id, a.cate_l2_id
  from test_info_tmp a
) d;

insert overwrite table $dim_app_category_mapping_par partition(version=1000)
select trim(regexp_replace(pkg, '\\n|\\r', '')) as pkg,
       trim(regexp_replace(apppkg, '\\n|\\r', '')) as apppkg,
       trim(regexp_replace(appname, '\\n|\\r', '')) as appname,
       trim(regexp_replace(cate_l1, '\\n|\\r', '')) as cate_l1,
       trim(regexp_replace(cate_l2, '\\n|\\r', '')) as cate_l2,
       trim(regexp_replace(cate_l1_id, '\\n|\\r', '')) as cate_l1_id,
       trim(regexp_replace(cate_l2_id, '\\n|\\r', '')) as cate_l2_id
from
(
  select a.pkg, a.apppkg, a.appname, a.cate_l1, a.cate_l2, a.cate_l1_id, a.cate_l2_id
  from test_info_tmp a
) d;
"

doubleCheckSql="select * from $dim_app_category_mapping_par where version='1000' and (cate_l1_id is null or cate_l2_id is null)"

spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.sortsystem.SortingDbUtils \
--driver-memory 6G \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.dynamicAllocation.executorIdleTimeout=20s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--executor-memory 12G --executor-cores 4 \
--name "SortingAppCagtegoryUtils" \
--conf spark.sql.shuffle.partitions=100 \
--conf spark.sql.autoBroadcastJoinThreshold=519715200 \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$mysqlInfoStr" "$mailList" "$insertTestTableSql" "$checkDataSqls" "$bakAndInsertSql" "$doubleCheckSql"
