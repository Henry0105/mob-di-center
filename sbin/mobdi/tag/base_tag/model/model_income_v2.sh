#!/bin/bash
set -e -x
: '
@owner:guanyt
@describe:设备的age预测v3
@projectName:mobdi
@BusinessName:model_age_v3
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dm_mobdi_tmp
input_table="${tmpdb}.income_new_features_all"

# output
output_table=$income_scoring_v2_result_di


# model path
model_path1="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income1.pmml"
model_path2="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income2.pmml"
model_path3="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income3.pmml"
model_path4="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income4.pmml"
model_path5="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income5.pmml"

model_path1_pre="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income1_pr.csv"
model_path2_pre="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income2_pr.csv"
model_path3_pre="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income3_pr.csv"
model_path4_pre="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income4_pr.csv"
model_path5_pre="hdfs://ShareSdkHadoop/dmgroup/dba/modelpath/20211203/income_v2/income5_pr.csv"


spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.IncomeScoreV2 \
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
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$input_table" "$model_path1" "$model_path2" \
"$model_path3" "$model_path4" "$model_path5" "$model_path1_pre" "$model_path2_pre" "$model_path3_pre" \
"$model_path4_pre" "$model_path5_pre" "$output_table" "$day"
