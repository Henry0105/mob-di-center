#!/bin/bash

set -e -x
day=$1
subject="[${day}-Mobdi_Monitor]数据行数和主键行数统计"
mysqlInfoStr="{\"userName\":\"root\",\"pwd\":\"mobtech2019java\",\"dbName\":\"mobdi_monitor\",\"host\":\"10.21.33.28\",\"port\":3306,\"tableName\":\"monitor_counts_online\",\"start_date\":$day,\"end_date\":$day,\"batchsize\":\"10000\",\"repartition\":\"8\"}"
#mailList="zhtli@mob.com"
mailList=$3

pday=`date -d "$day  1 days ago "  "+%Y%m%d"`
#FORMAT(total_counts,0),千分位
src="(select a.id,a.show_style,a.table_name,a.key_info,a.table_type,a.partition_info,FORMAT(a.total_counts,0) as total_counts,FORMAT(a.key_counts,0) as key_counts,FORMAT(b.total_counts,0) as yesterday_total_counts,
FORMAT(b.key_counts,0) as yesterday_key_counts,a.total_counts/b.total_counts as total_counts_ratio,a.stats_date
from 
(select * from monitor_counts_online where stats_date=$day)a 
left join
(select * from monitor_counts_online where stats_date=$pday) b 
on a.table_name =b.table_name)src"

debug="(select * from monitor_counts_online where stats_date=$day)src"

spark2-submit --master local \
            --class com.mob.mobdi.utils.MonitorReport \
            --name mobdi_monitor_report \
            --driver-memory 4g \
            --executor-memory 12G \
            --executor-cores 4 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=1 \
            --conf spark.dynamicAllocation.maxExecutors=20 \
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
            /home/dba/lib/MobDI_Monitor-1.0-SNAPSHOT-jar-with-dependencies.jar $day " $mysqlInfoStr" $subject $mailList "$src"
