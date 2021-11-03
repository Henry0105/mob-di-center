#!/bin/bash

set -e -x

day=$1

spark2-submit \
	--master yarn \
  --queue root.important \
	--executor-memory 15G \
	--driver-memory 8G \
	--executor-cores 3 \
  --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+UseG1GC " \
  --conf spark.rpc.askTimeout=500 \
  --conf spark.dynamicAllocation.maxExecutors=300 \
	--conf spark.default.parallelism=4000 \
	--conf spark.sql.shuffle.partitions=4000 \
	--name "unique_id_incr" \
	--deploy-mode cluster \
	--class com.mob.deviceid.gen.GenDeviceIdInc \
	--conf spark.executor.memoryOverhead=10240 \
	--conf spark.driver.maxResultSize=8g \
	/home/dba/mobdi_center/lib/MobDI_Muid-1.0-SNAPSHOT-jar-with-dependencies.jar $day