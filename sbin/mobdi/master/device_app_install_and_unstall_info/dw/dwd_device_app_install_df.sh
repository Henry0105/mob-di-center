#!/bin/bash

set -e -x

day=$1

spark2-submit --master yarn \
--executor-memory 9G \
--driver-memory 6G \
--executor-cores 3 \
--name "app_install" \
--deploy-mode cluster \
--class com.youzu.mob.stall.StallDataWare \
--conf spark.sql.shuffle.partitions=100 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.driver.maxResultSize=5g \
/home/dba/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar $day "appInstallDF"
