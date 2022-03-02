#!/usr/bin/env bash

set -e -x

day=$1

spark2-submit --master yarn \
--executor-memory 9G \
--driver-memory 9G \
--executor-cores 3 \
--class com.youzu.mob.ppx.PPXScore \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=50 \
--conf spark.network.timeout=1200s \
--conf spark.executor.heartbeatInterval=30s \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.driver.maxResultSize=5g \
--jars /home/dba/lib/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar ${day}