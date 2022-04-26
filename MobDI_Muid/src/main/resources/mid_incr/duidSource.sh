#!/bin/bash

: '
@owner:luost
@DESCribe:mid增量合并
@projectName:MOBDI
'

set -e -x 

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
pday=`date -d "$day -1 day" +%Y%m%d`

spark2-submit --master yarn \
--deploy-mode cluster \
--driver-memory 6G \
--executor-memory 20G \
--executor-cores 6 \
--queue root.important \
--class com.mob.mid_incr_v2.DuidSource \
--conf spark.dynamicAllocation.maxExecutors=400 \
--conf spark.dynamicAllocation.minExecutors=100 \
--conf spark.default.parallelism=4000 \
--conf spark.sql.shuffle.partitions=4000 \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.driver.maxResultSize=2g \
--conf spark.kryoserializer.buffer.max=128m \
--conf spark.network.maxRemoteBlockSizeFetchToMem=10m \
--conf spark.shuffle.accurateBlockThreshold=10m \
--conf spark.rpc.askTimeout=500 \
--conf spark.task.maxFailures=6 \
--conf spark.reducer.maxSizeInFlight=96m \
--conf spark.shuffle.file.buffer=128k \
--conf spark.speculation.quantile=0.98 \
--jars hdfs://ShareSdkHadoop/user/dba/yanhw/etl_udf-1.1.2.jar \
/home/dba/luost/MobDI_Muid-1.0-SNAPSHOT-jar-with-dependencies.jar $day $pday
