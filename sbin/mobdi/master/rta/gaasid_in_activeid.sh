#!/bin/bash

: '
@describe:1.从pv与mdata表，取ieid/oiid，apppkg维度的月活 2.mob月活数据与rta设备池数据取交集
'


#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh


/opt/mobdata/sbin/spark-submit --master yarn \
--executor-memory 40G \
--driver-memory 15G \
--executor-cores 6 \
--conf "spark.dynamicAllocation.minExecutors=50" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.initialExecutors=50" \
--name "gaasid_in_activeid" \
--deploy-mode cluster \
--class com.youzu.mob.rta.gaasid_in_activeid \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.network.timeout=300000 \
--conf spark.core.connection.ack.wait.timeout=300000 \
--conf spark.akka.timeout=300000 \
--conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
--conf spark.shuffle.io.connectionTimeout=300000 \
--conf spark.rpc.askTimeout=300000 \
--conf spark.rpc.lookupTimeout=300000 \
--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
--queue root.sdk.mobdashboard_test \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar