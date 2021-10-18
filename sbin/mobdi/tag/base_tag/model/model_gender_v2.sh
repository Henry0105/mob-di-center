#!/bin/bash
set -e -x

MobDI_HOME="$(cd "`dirname "$0"`"/../../../../..; pwd)"

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit  1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

# input
label_gender_feature="$dm_mobdi_tmp.gender_feature_v2_final"
model_path="/dmgroup/dba/modelpath/20210730/gender/gender_xgb_model.pmml"

# output
outputTable=$gender_scoring_result_di

tmp_sql="select * from  $label_gender_feature where day=${day}"


spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.GenderScoreV2 \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.driver.maxResultSize=4g \
--conf spark.speculation=true \
--conf spark.network.timeout=1000000 \
--conf spark.executor.heartbeatInterval=60s \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$day" "$outputTable" "$tmp_sql" "$model_path"





