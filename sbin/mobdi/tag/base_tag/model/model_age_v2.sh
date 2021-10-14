#!/bin/bash
set -e -x
: '
@owner:guanyt
@describe:设备的age预测v2
@projectName:mobdi
@BusinessName:model_age
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

tmpdb=$dw_mobdi_tmp
## input
tmp_score_part1="${tmpdb}.tmp_score_part1"
tmp_score_part3="${tmpdb}.tmp_score_part3"
tmp_score_part4="${tmpdb}.tmp_score_part4"
tmp_score_part5="${tmpdb}.tmp_score_part5"
tmp_score_part6="${tmpdb}.tmp_score_part6"
tmp_score_part2="${tmpdb}.tmp_score_part2"
tmp_score_app2vec="${tmpdb}.tmp_score_app2vec"

##output

modelPath="/dmgroup/dba/modelpath/20200811/linear_regression_model/agemodel"
modelPath0="/dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_0"
modelPath1="/dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_1"
modelPath2="/dmgroup/dba/modelpath/20200811/linear_regression_model/age1001model_2"

threshold="1.0, 0.9, 2.0, 1.7, 0.9"
out_put_table=$label_l2_result_scoring_di
test_flag="0"

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.AgeScoreV2 \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$modelPath0" "$modelPath1" "$modelPath2" "$threshold" "$out_put_table" "20200701" "$day" $test_flag
