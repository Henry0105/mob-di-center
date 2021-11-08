#!/bin/bash
set -e -x
: '
@owner:hugl
@describe:income1001预测v2
@projectName:mobdi
@BusinessName:model_income1001_v2
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

#input
tmp_score_part1="${tmpdb}.tmp_score_part1"
tmp_score_part2="${tmpdb}.tmp_income1001_part2"
tmp_score_part3="${tmpdb}.tmp_score_part3"
tmp_score_part4="${tmpdb}.tmp_income1001_part4"
tmp_score_part5="${tmpdb}.tmp_score_part5_v3"
tmp_score_part6="${tmpdb}.tmp_score_part6_v3"
tmp_score_part7="${tmpdb}.tmp_income1001_part7"
tmp_score_part8="${tmpdb}.tmp_income1001_part8"
tmp_score_app2vec="${tmpdb}.tmp_score_app2vec"


modelPath="/user/xinzhou/model20201123/income_lr_fix3"

threshold="1.8,1,0.6,0.65,0.9"
out_put_table="$label_l2_result_scoring_di"

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.Income1001ScoreV2 \
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
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
 --conf spark.default.parallelism=2000 \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$tmp_score_part1" "$tmp_score_part2" "$tmp_score_part3" "$tmp_score_part4" "$tmp_score_part5" "$tmp_score_part6" "$tmp_score_part7" "$tmp_score_part8" "$tmp_score_app2vec" "$modelPath" "$threshold" "$out_put_table" "$day"
