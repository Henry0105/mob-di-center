#!/bin/bash

set -e -x

if [ $# -eq 2 ]; then
day=$1
timewindow=$2
p1=`date +%Y%m%d -d "${day} -1 day"`
p180=`date +%Y%m%d -d "${day} -${timewindow} day"`
else
day=$1
p1=`date +%Y%m%d -d "${day} -1 day"`
p180=`date +%Y%m%d -d "${day} -180 day"`
fi

/opt/mobdata/sbin/spark-submit --master yarn \
--executor-memory 10G \
--driver-memory 4G \
--executor-cores 3 \
--conf spark.dynamicAllocation.maxExecutors=400 \
--conf spark.default.parallelism=6000 \
--conf spark.sql.shuffle.partitions=6000 \
--name "app_status" \
--deploy-mode cluster \
--class com.youzu.mob.stall.StallDataWareService \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar $day $p1 $p180 "statusStall"
