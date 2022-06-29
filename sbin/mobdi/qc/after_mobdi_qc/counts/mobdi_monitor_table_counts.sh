#!/bin/bash

set -e -x

if [ $# -lt 6 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<table> <key> <day> <tableType> <parInfo> <show_style>'"
     exit 1
fi

table=$1
key=$2
day=$3
mysqlInfoStr="{\"userName\":\"root\",\"pwd\":\"mobtech2019java\",\"dbName\":\"mobdi_monitor\",\"host\":\"10.21.33.28\",\"port\":3306,\"tableName\":\"monitor_counts_online\",\"start_date\":$day,\"end_date\":$day,\"batchsize\":\"10000\",\"repartition\":\"8\"}"
tableType=$4
parInfo=$5
show_style=$6
table_name=`echo "$table" | awk '{print $1}'`
echo $table_name


spark2-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.mobdi.utils.MonitorCounts \
            --name mobdi_monitor \
            --driver-memory 8g \
            --executor-memory 12G \
            --executor-cores 4 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=30 \
            --conf spark.dynamicAllocation.maxExecutors=150 \
            --conf spark.dynamicAllocation.executorIdleTimeout=15s \
            --conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
            --conf spark.yarn.executor.memoryOverhead=4096 \
            --conf spark.kryoserializer.buffer.max=1024 \
            --conf spark.default.parallelism=500 \
            --conf spark.sql.shuffle.partitions=500 \
            --conf spark.driver.maxResultSize=4g \
            --conf spark.storage.memoryFraction=0.4 \
            --conf spark.shuffle.memoryFraction=0.4 \
            --driver-java-options "-XX:MaxPermSize=1024m" \
            /home/dba/lib/MobDI_Monitor-1.0-SNAPSHOT-jar-with-dependencies.jar "$table" "$key" $mysqlInfoStr $day $tableType $parInfo $show_style
