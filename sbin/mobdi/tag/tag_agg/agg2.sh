#!/usr/bin/env bash

set -e -x

: '
  将多个年分区, 合并成一个 新的年分区
  可手动调动，用于快速生成索引
'

inputTable=$3

version_list=$1
version_out=$2

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit-dev --executor-memory 30g        \
--master yarn        \
--executor-cores 4        \
--queue root.important	     \
--name profile_index_agg2        \
--deploy-mode cluster        \
--class com.youzu.mob.profile.OrcIndexAggregator2        \
--driver-memory 4g        \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.dynamicAllocation.enabled=true"        \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"        \
--conf "spark.dynamicAllocation.minExecutors=1"        \
--conf "spark.dynamicAllocation.initialExecutors=1"        \
--conf "spark.speculation.quantile=0.98"        \
--conf "spark.yarn.executor.memoryOverhead=7168" \
--conf "spark.dynamicAllocation.maxExecutors=400"    \
--conf "spark.sql.shuffle.partitions=20000"     \
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.yarn.executor.memoryOverhead=4g" \
--conf "spark.speculation=true"        \
--conf "spark.shuffle.service.enabled=true"        \
/home/dba/mobdi_center/lib/v6_muid/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $version_list $version_out $inputTable