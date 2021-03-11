#!/bin/sh
set -x -e
day=$1
p90day=`date -d "$day -90 days" +%Y%m%d`
date=`date -d "$day" +%d`
if [ $date -eq 01 ]
then
	spark2-submit --master yarn \
			  --executor-memory 15G \
			  --driver-memory 15G \
			  --executor-cores 5  \
			  --class com.youzu.mob.permanent.Permanent \
			  --conf spark.dynamicAllocation.enabled=true \
			  --conf spark.dynamicAllocation.minExecutors=70\
			  --conf spark.dynamicAllocation.maxExecutors=130 \
			  --conf spark.default.parallelism=1800 \
			  --conf spark.sql.shuffle.partitions=3000 \
			  --conf spark.yarn.executor.memoryOverhead=2048 \
			  --conf spark.network.timeout=200s \
			  --conf spark.executor.heartbeatInterval=30s \
			  /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$day" "$p90day"
else
	echo $day
fi
