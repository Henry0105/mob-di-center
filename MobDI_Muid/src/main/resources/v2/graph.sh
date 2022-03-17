#!/bin/bash
set -x -e

tmp_db=dm_mid_master

duid_fsid_mapping=$tmp_db.duid_unid_mapping

ids_unid_vertex="$tmp_db.ids_unid_vertex"

ids_graph_result_full="$tmp_db.ids_graph_result_full"

app_unid_final_mapping="$tmp_db.old_new_unid_mapping_par"

all_graph_result="$tmp_db.all_graph_result"

hive -e "
create table if not exists $ids_graph_result_full (
old_id string,
new_id string
) stored as orc;
create table if not exists $all_graph_result like $ids_graph_result_full;
"

source_sql_ids="select id1,id2 from $ids_unid_vertex where day='all'"

/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--name Step2TokenConnectedComponents_ids \
--class com.mob.mid_full.TokenConnectedComponentsNew \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=300 \
--conf spark.sql.shuffle.partitions=300 \
--executor-memory 30g \
--executor-cores 2 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=256m \
--conf spark.shuffle.accurateBlockThreshold=256m \
./muid.jar 15 "$source_sql_ids" $ids_graph_result_full


hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
SET hive.exec.parallel=true;

insert overwrite table $ids_unid_vertex partition(day='all+app')
select id1,id2 from (
select old_id id1,new_id id2 from $ids_graph_result_full
union all
select old_id id1,new_id id2 from $app_unid_final_mapping where month='2019-2021')t
group by id1,id2
"
source_sql_all="select id1,id2 from $ids_unid_vertex where day='all+app'"

/opt/mobdata/sbin/spark-submit --master yarn \
--deploy-mode cluster \
--name Step2TokenConnectedComponents_ids \
--class com.mob.mid_full.TokenConnectedComponentsNew \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--executor-memory 30g \
--executor-cores 2 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.driver.maxResultSize=1024m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=256m \
--conf spark.shuffle.accurateBlockThreshold=256m \
./muid.jar 15 "$source_sql_all" $all_graph_result

