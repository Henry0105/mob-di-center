#!/bin/bash

: '步骤
1. RTA最终设备池打一、二级标签,向RTA设备标签表插入数据（因在装应用包名未清洗，所以需先跟数仓的映射表映射取到清洗后的包名(优先取清洗后的包名，未清洗的则保留   dw_device_with_cate_table
2. 向RTA正负向设备表插入设备信息及二级游戏分类的应用数    dw_device_with_cate_installcount_table
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

#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#app_category_mapping_par=dm_sdk_mapping.app_category_mapping_par

#获取最新分区
dim_app_pkg_par=`hive -e "show partitions $dim_app_pkg_mapping_par;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`
app_category_par=`hive -e "show partitions $app_category_mapping_par;"  |sort -rn | awk -F "=" '{print $2}' | head -n 1`
echo "$dim_app_pkg_par"
echo "$app_category_par"

/opt/mobdata/sbin/spark-submit --master yarn \
--executor-memory 20G \
--driver-memory 6G \
--executor-cores 3 \
--conf "spark.dynamicAllocation.minExecutors=50" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.dynamicAllocation.initialExecutors=50" \
--name "device_with_cate_installcount" \
--deploy-mode cluster \
--class com.youzu.mob.rta.device_with_cate_installcount \
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
--conf spark.sql.autoBroadcastJoinThreshold=104857600 \
--queue root.sdk.mobdashboard_test \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar $insert_day $dim_app_pkg_par $app_category_par