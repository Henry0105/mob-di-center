#!/bin/bash

set -e -x

: '
@owner:zhtli
@describe: 实现分拣系统中，将app标签分类从MySQL导入hive中去，并check数据，如果数据无效，则发邮件
@projectName:SortingSystem
'
source /home/dba/mobdi_center/conf/hive-env.sh

#dim_p2p_app_cat_par=dim_sdk_mapping.dim_p2p_app_cat_par
p2p_app_cat_db=${dim_p2p_app_cat_par%.*}
p2p_app_cat_tb=${dim_p2p_app_cat_par#*.}


result=`mysql -h10.21.33.28 -u root -p'mobtech2019java' -P3306  -e "select max(time) from sorting_system.multiloan" -sN`
targetPar=1000.$result

parSql="
    add jar hdfs://ShareSdkHadoop/user/haom/udf/original-hive_udf-1.0.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('$p2p_app_cat_db','$p2p_app_cat_tb', 'version');
    drop temporary function GET_LAST_PARTITION;
"
lastPartition=$(hive -e "$parSql" -SN)

if [ "$lastPartition" \< "$targetPar" ];then
  echo "更新mapping表数据"
else
  exit 0
fi

mysqlInfoStr='{"userName":"root","pwd":"mobtech2019java","dbName":"sorting_system","host":"10.21.33.28","port":3306,"tableName":"multiloan"}'
mailList="DIMonitor@mob.com;yqzhou@mob.com"

#其中mysql_table_info_tmp表是spark临时表，用于存储mysql拉取过来的数据
#sql结果存入spark临时表test_info_tmp中
insertTestTableSql="
select c.pkg, c.apppkg_new as apppkg, c.cat, c.cate_id
from
(
  select a.pkg, case when a.apppkg is null or length(trim(a.apppkg))=0 then a.pkg else a.apppkg end as apppkg_new, a.cate_l3 as cat, trim(b.cate_id) as cate_id
  from mysql_table_info_tmp a
  left join
  (
    select cate_id,trim(cat) as cat
    from $dim_p2p_app_cat_par
    where version='1000'
    group by cate_id,trim(cat)
  ) b on trim(a.cate_l3)=trim(b.cat)
) c
where c.cate_id is not null
group by c.pkg, c.apppkg_new, c.cat, c.cate_id
"

checkDataSqls="
--校验同一个apppkg是否出现了多次不同分类
select *
from test_info_tmp c
where c.apppkg in (
  select apppkg
  from
  (
    select apppkg, count(cat) as cnt_cat, count(cate_id) as cnt_cate_id
    from
    (
      select apppkg, trim(cat) as cat, trim(cate_id) as cate_id
      from test_info_tmp
      group by apppkg, trim(cat), trim(cate_id)
    ) a
    group by apppkg
  ) b
  where b.cnt_cat>=2 or b.cnt_cate_id>=2
)
"

bakAndInsertSql="
insert overwrite table $dim_p2p_app_cat_par partition(version=$targetPar)
select a.apppkg, a.cat, a.cate_id 
from test_info_tmp a
where length(a.pkg)>0
and a.pkg not in (
  select pkg
  from
  (
    select pkg
    from test_info_tmp
    where cat is null or cate_id is null
  ) b
  group by pkg
);

insert overwrite table $dim_p2p_app_cat_par partition(version=1000)
select pkg, cat, cate_id
from $dim_p2p_app_cat_par
where version='$targetPar'
"

doubleCheckSql="
select *
from $dim_p2p_app_cat_par
where version='1000'
and (cat is null or cate_id is null) 
"

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
--name "SortingLoanAppCagtegoryUtils" \
--conf spark.sql.shuffle.partitions=100 \
--conf spark.sql.autoBroadcastJoinThreshold=519715200 \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$mysqlInfoStr" "$mailList" "$insertTestTableSql" "$checkDataSqls" "$bakAndInsertSql" "$doubleCheckSql"
