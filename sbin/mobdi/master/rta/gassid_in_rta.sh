#!/bin/bash

: '
@describe:
1. 从dm_mobdi_master.dwd_gaas_rta_id_data_di中提取出1周内的去重ieid、oiid数据，入结果表     dw_gaas_id_data_di_rta_table
2. 更新RTA异常设备池(对应oiid数>2的imei，及对应ieid数>3的oiid均入黑名单，需继承历史黑名单)     dw_gaasid_blacklist_table
3. RTA设备信息池(RTA请求设备信息去重表过滤黑名单，不继承历史数据)     dw_gaasid_final_table
'
insert_day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

full_par=`hive -e "show partitions $dw_gaasid_blacklist;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`

/opt/mobdata/sbin/spark-submit --master yarn \
--executor-memory 20G \
--driver-memory 6G \
--executor-cores 3 \
--conf "spark.dynamicAllocation.minExecutors=50" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.initialExecutors=50" \
--name "gassid_in_rta" \
--deploy-mode cluster \
--class com.youzu.mob.rta.gassid_in_rta \
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