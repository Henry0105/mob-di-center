#!/usr/bin/env bash

set -x -e


start=$1
end=$2
out_table=$3

HADOOP_USER_NAME=dba spark2-submit \
--executor-memory 10g        \
--master yarn        \
--executor-cores 3        \
--queue dba     \
--name profile_update_atomic_partition_${start}_${end}       \
--deploy-mode cluster        \
--class com.youzu.mob.profile.OrcIndexOnline        \
--driver-memory 2g        \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.dynamicAllocation.enabled=true"        \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"        \
--conf "spark.dynamicAllocation.minExecutors=50"    \
--conf "spark.dynamicAllocation.maxExecutors=200"     \
--conf "spark.dynamicAllocation.initialExecutors=50"        \
--conf "spark.default.parallelism=1500" \
--conf "spark.sql.shuffle.partitions=1500" \
--conf "spark.speculation.quantile=0.98"        \
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.speculation=true"        \
--conf "spark.shuffle.service.enabled=true"        \
--conf "spark.yarn.executor.memoryOverhead=4096" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar  $start $end $out_table ""
