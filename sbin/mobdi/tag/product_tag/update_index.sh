#!/usr/bin/env bash

day=$1

echo "$day"

out_table=$2

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit-dev \
--executor-memory 10g        \
--master yarn        \
--executor-cores 4        \
--queue dba     \
--name profile_update_into_all_index_tmp_${day}_${day}       \
--deploy-mode cluster        \
--class com.youzu.mob.profile.OrcIndexTmpUpdater        \
--driver-memory 2g        \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.dynamicAllocation.enabled=true"        \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"        \
--conf "spark.dynamicAllocation.minExecutors=1"        \
--conf "spark.dynamicAllocation.initialExecutors=1"        \
--conf "spark.speculation.quantile=0.98"        \
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.speculation=true"        \
--conf "spark.shuffle.service.enabled=true"        \
--conf "spark.dynamicAllocation.maxExecutors=200"    \
--conf "spark.yarn.executor.memoryOverhead=4096" \
--conf "spark.sql.shuffle.partitions=12000"     \
/home/dba/mobdi_center/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar  $day $day $out_table  ""
