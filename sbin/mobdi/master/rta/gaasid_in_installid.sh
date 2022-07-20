#!/bin/bash

: '
@describe:
1. 插入30天内包名不为空且ieid/oiid任一不为空的在装应数据     dw_install_app_all_rta_table
2. RTA设备池与在装应用清洗表取交集      dw_gaasid_in_installid_table
'

insert_day=$1
#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh


/opt/mobdata/sbin/spark-submit --master yarn \
--executor-memory 20G \
--driver-memory 6G \
--executor-cores 3 \
--conf "spark.dynamicAllocation.minExecutors=50" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.initialExecutors=50" \
--name "gaasid_in_installid" \
--deploy-mode cluster \
--class com.youzu.mob.rta.gaasid_in_installid \
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
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $insert_day