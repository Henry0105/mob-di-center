#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 周更的通讯录映射表
@projectName:MOBDI
'
# 是否需要修改

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

#input
mapdb="dm_mobdi_mapping"
model_path="/dmgroup/dba/modelpath/20200810/mapping_age_contact_word2vec"

#output
output_table="${mapdb}.mapping_contacts_word2vec2"


hive -v -e "
create table if not exists $output_table
(
  phone string COMMENT '通讯录手机号',
  w2v_100 array<double> COMMENT 'array double的向量'
) partitioned by
(day string)
stored as orc;
"

phone_contact_version=(`hive  -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dw_mobdi_md', 'phone_contacts_index_word_split_prepare', 'day');
"`)


spark2-submit --master yarn  --deploy-mode cluster  \
--class com.youzu.mob.newscore.MappingAgeContactWord2Vec \
--driver-memory 8G --num-executors 5 \
--executor-cores 2 \
--executor-memory 10G  \
--conf spark.default.parallelism=400 \
--conf spark.storage.memoryFraction=0.7 --conf spark.shuffle.memoryFraction=0.05 \
--conf spark.driver.maxResultSize=5G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.driver.memory=2G \
--conf spark.shuffle.service.enabled=true \
--jars /home/dba/lib/ansj_seg-5.1.6.jar,/home/dba/lib/nlp-lang-1.7.7.jar \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $day $phone_contact_version $model_path $output_table

hive -v -e "
create or replace view ${mapdb}.mapping_contacts_word2vec2_view as
select phone,w2v_100 from ${mapdb}.mapping_contacts_word2vec2 where day='$day'
"