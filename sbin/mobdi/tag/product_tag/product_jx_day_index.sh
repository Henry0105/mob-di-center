#!/bin/bash

set -x -e

day=$1

echo "$day"


outputTable="dm_mobdi_report.timewindow_online_profile_ronghui_product_jx"


HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit \
--master yarn        \
--executor-cores 4        \
--queue dba     \
--name profile_day_index_gen_${day}_${day}        \
--deploy-mode cluster        \
--executor-memory 4G \
--conf "spark.yarn.executor.memoryOverhead=4G" \
--class com.youzu.mob.profile.OrcIndexWriter        \
--driver-memory 2g   \
--conf "spark.yarn.maxAppAttempts=1"     \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.dynamicAllocation.enabled=true"        \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC"        \
--conf "spark.dynamicAllocation.minExecutors=1"        \
--conf "spark.dynamicAllocation.initialExecutors=1"        \
--conf "spark.speculation.quantile=0.98"        \
--conf "spark.executorEnv.JAVA_HOME=/opt/jdk1.8.0_45"        \
--conf "spark.speculation=true"        \
--conf "spark.shuffle.service.enabled=true"        \
/home/dba/mobdi_center/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar ${day} ${day} $outputTable ""
