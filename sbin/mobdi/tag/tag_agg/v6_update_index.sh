#!/usr/bin/env bash

##每日更新索引数据到年份的大索引中，但是生成在year.00xx分区，等飞飞的hbase好了之后，这边rename到10xx分区

start=$1
end=$2

inputTable="dm_mobdi_report.timewindow_online_profile_day_v6"

prefix=""

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit-dev --executor-memory 30g        \
--master yarn        \
--executor-cores 4        \
--queue root.yarn_data_compliance2     \
--name profile_update_into_all_index_tmp_${start}_${end}       \
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
--conf "spark.yarn.executor.memoryOverhead=4g" \
--conf "spark.sql.shuffle.partitions=12000"     \
/home/dba/mobdi_center/lib/v6_muid/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar  $start $end $inputTable "$prefix"
