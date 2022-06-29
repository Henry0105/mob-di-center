#!/bin/bash
set -x -e

: '
@owner:guanyt
@describe:id_mapping表的qc，qc不通过则不生成视图
@projectName:QC
'

: '
  第一批qc表：
  dm_mobdi_mapping.android_id_full、dm_mobdi_mapping.ios_id_full
  1.统计表的当前分区总数、当前分区主键总数、上一个分区总数
  2.如果当前分区主键数不等于当前分区总数或者当前分区总数小于等于上一个分区总数或者与上一个分区相比数据量波动超过10%，
    表示qc失败，程序已失败退出
  3.qc成功，返回
'

if [[ $# -lt 2 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: <table>, <table_par>"
     exit 1
fi



table=$1
table_par=$2
last_table_par=$3

spark2-submit --master yarn \
            --deploy-mode cluster \
            --name mobdi_monitor_id_mapping_merge \
            --driver-memory 8g \
            --executor-memory 12G \
            --executor-cores 4 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=30 \
            --conf spark.dynamicAllocation.maxExecutors=150 \
            --conf spark.dynamicAllocation.executorIdleTimeout=15s \
            --conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
            --conf spark.executor.memoryOverhead=4096 \
            --conf spark.kryoserializer.buffer.max=1024 \
            --conf spark.default.parallelism=500 \
            --conf spark.sql.shuffle.partitions=500 \
            --conf spark.driver.maxResultSize=4g \
            --conf spark.storage.memoryFraction=0.4 \
            --conf spark.shuffle.memoryFraction=0.4 \
            --conf spark.yarn.maxAppAttempts=0 \
            --driver-java-options "-XX:MaxPermSize=1024m" \
            /home/dba/mobdi_center/qc/real_time_mobdi_qc/qc_id_mapping_merge.py $table $table_par $last_table_par $last_table_par