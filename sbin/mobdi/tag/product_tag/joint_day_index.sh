#!/bin/bash

set -x

: '
  金融线定制标签 joint_product1 索引
'

day=$1


outputTable="dm_mobdi_report.timewindow_online_profile_joint_product1"

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit-dev \
--master yarn        \
--executor-cores 3        \
--queue root.important     \
--name profile_day_index_gen_${day}_${day}        \
--deploy-mode cluster        \
--executor-memory 8G \
--conf "spark.yarn.executor.memoryOverhead=4G" \
--class com.youzu.mob.profile.OrcIndexWriter        \
--driver-memory 2g   \
--conf spark.yarn.maxAppAttempts=1     \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.dynamicAllocation.enabled=true"        \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"        \
--conf "spark.dynamicAllocation.minExecutors=50"    \
--conf "spark.dynamicAllocation.maxExecutors=200"      \
--conf "spark.dynamicAllocation.initialExecutors=50"        \
--conf "spark.default.parallelism=1500"     \
--conf "spark.sql.shuffle.partitions=1500"    \
--conf "spark.speculation.quantile=0.98"        \
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.speculation=true"        \
--conf "spark.shuffle.service.enabled=true"        \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar ${day} ${day} $outputTable ""
