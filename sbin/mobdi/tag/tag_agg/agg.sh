#!/usr/bin/env bash

set -e -x

: '
  将多个日索引，合并生成一个 年索引分区
  可手动调动，用于快速生成索引
'

start=$1
end=$2
version=$3
inputTable=$4


HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit-dev --executor-memory 20g        \
--master yarn        \
--executor-cores 3        \
--queue root.yarn_data_compliance2     \
--name profile_index_agg_${day}        \
--deploy-mode cluster        \
--class com.youzu.mob.profile.OrcIndexAggregator        \
--driver-memory 2g        \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.dynamicAllocation.enabled=true"        \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"        \
--conf "spark.dynamicAllocation.minExecutors=1"        \
--conf "spark.dynamicAllocation.initialExecutors=1"        \
--conf "spark.speculation.quantile=0.98"        \
--conf "spark.yarn.executor.memoryOverhead=2048" \
--conf "spark.dynamicAllocation.maxExecutors=400"    \
--conf "spark.sql.shuffle.partitions=1000"     \
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.speculation=true"        \
--conf "spark.shuffle.service.enabled=true"        \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $start $end $version $inputTable
