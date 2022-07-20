#!/bin/bash

: '步骤
1. 新建临时表，包括本次所有0327推荐设备数据     dm_device_rec_for_0327_pre_table
2. 更新至0327规则结果表最新分区，增加增改、删、不变的状态     dm_device_rec_for_0327_table
'

set -e -x

if [[ $# -lt 1 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

insert_day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh


#获取最新分区
full_par=`hive -e "show partitions $dm_device_rec_for_0327;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`


/opt/mobdata/sbin/spark-submit --master yarn \
--executor-memory 20G \
--driver-memory 6G \
--executor-cores 3 \
--conf "spark.dynamicAllocation.minExecutors=50" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.initialExecutors=50" \
--name "rta_device_rec_for_0327" \
--deploy-mode cluster \
--class com.youzu.mob.rta.rta_device_rec_for_0327 \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.executor.memoryOverhead=4096 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.network.timeout=300000 \
--conf spark.core.connection.ack.wait.timeout=300000 \
--conf spark.akka.timeout=300000 \
--conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
--conf spark.shuffle.io.connectionTimeout=300000 \
--conf spark.rpc.askTimeout=300000 \
--conf spark.rpc.lookupTimeout=300000 \
--queue root.sdk.mobdashboard_test \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $insert_day $full_par