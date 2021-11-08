#!/bin/bash
set -e -x

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
day=$1
tmpdb=$dm_mobdi_tmp
#input
label_age_feature="${tmpdb}.age_new_features_all"

precisionTable1="${tmpdb}.age_below_18_pr_table"
precisionTable2="${tmpdb}.age_18_24_pr_table"
precisionTable3="${tmpdb}.age_25_34_pr_table"
precisionTable4="${tmpdb}.age_35_44_pr_table"
precisionTable5="${tmpdb}.age_45_54_pr_table"
precisionTable6="${tmpdb}.age_beyond_54_pr_table"

#output
outputTable=$age_scoring_v4_result_di

#source
sourceData="select * from $label_age_feature where day=${day}"


spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.AgeScoreV4 \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=500 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=4096 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.default.parallelism=2000 \
--conf spark.network.timeout=1000000 \
--conf spark.executor.heartbeatInterval=60s \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$day" \
"$sourceData" "$outputTable" "$precisionTable1" "$precisionTable2" "$precisionTable3" \
"$precisionTable4" "$precisionTable5" "$precisionTable6"