#!/bin/bash

set -e -x
 : '
  实现监控kv类型值的左右两边值的长度是否相等的功能，比如：a,b,c=1,2,3 是正确的;a,b,c,d=1,2,3 则是错误的
 '
table=$1
key=$2
kvLists=$3

mailList="zhtli@mob.com;zhtli@mob.com"
limits=100000

   function log_info(){

    DATE_N=`date "+%Y-%m-%d %H:%M:%S"`
     echo "$DATE_N $@"
  }

  log_info "start check $table"

spark2-submit --master yarn \
            --deploy-mode cluster \
            --class com.mob.mobdi.utils.MonitorKVPairs \
            --name mobdi_monitor \
            --driver-memory 8g \
            --executor-memory 12G \
            --executor-cores 4 \
            --conf spark.driver.maxResultSize=4G \
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
            --conf spark.storage.memoryFraction=0.4 \
            --conf spark.shuffle.memoryFraction=0.4 \
            --driver-java-options "-XX:MaxPermSize=1024m" \
            /home/dba/lib/MobDI_Monitor-1.0-SNAPSHOT-jar-with-dependencies.jar "$table" "$key" "$kvLists" "$mailList" $limits


log_info "$table check over"