#!/bin/bash

set -e -x

source /home/dba/mobdi_center/conf/hive-env.sh
tmp_device_frequency_place=$dm_mobdi_tmp.tmp_device_frequency_place
# 删除临时分区
hive -e"
alter table $tmp_device_frequency_place drop partition(stage='A');
alter table $tmp_device_frequency_place drop partition(stage='B');
alter table $tmp_device_frequency_place drop partition(stage='C');
alter table $tmp_device_frequency_place drop partition(stage='D');
"

spark2-submit --master yarn \
		      --executor-memory 9G \
			  --driver-memory 15G \
			  --executor-cores 3 \
        --name "frequency A" \
        --deploy-mode cluster \
			  --class com.youzu.mob.location.frequency.Frequency \
			  --conf spark.dynamicAllocation.enabled=true \
			  --conf spark.dynamicAllocation.minExecutors=10 \
        --conf spark.network.timeout=1200s \
        --conf spark.executor.heartbeatInterval=30s \
        --conf spark.yarn.executor.memoryOverhead=4096 \
			  --conf spark.dynamicAllocation.maxExecutors=100 \
			  --conf spark.default.parallelism=10000 \
			  --conf spark.sql.shuffle.partitions=10000 \
			  --conf spark.driver.maxResultSize=5g \
        --conf spark.sql.adaptive.enabled=true \
         --conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=256000000 \
			  /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar A "" 1
