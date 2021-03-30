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

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"


label_age_unstall_feature="$appdb.label_age_app_unstall_1y"
label_age_pre_app_tgi_feature="$appdb.label_age_pre_app_tgi_feature"
label_age_applist_part2_beforechi="$appdb.label_age_applist_part2_beforechi"

#input

tmp_score_part1="${tmpdb}.tmp_score_part1"
tmp_score_part3="${tmpdb}.tmp_score_part3"
tmp_score_part4="${tmpdb}.tmp_score_part4"
tmp_score_part5="${tmpdb}.tmp_score_part5_v3"
tmp_score_part6="${tmpdb}.tmp_score_part6_v3"

tmp_score_part2_v3="${tmpdb}.tmp_score_part2_v3"
tmp_score_app2vec="${tmpdb}.tmp_score_app2vec_v3"

tmp_score_part7="${tmpdb}.tmp_score_part7"
tmp_score_part8="${tmpdb}.tmp_score_part8"



modelPath="/dmgroup/dba/modelpath/20201222/linear_regression_model/agemodel"
modelPath0="/dmgroup/dba/modelpath/20201222/linear_regression_model/age1001model_0"
modelPath1="/dmgroup/dba/modelpath/20201222/linear_regression_model/age1001model_1"
modelPath2="/dmgroup/dba/modelpath/20201222/linear_regression_model/age1001model_2"

#0.6,1.6,1.05,0.7,0.2
threshold="0.9,1.2,1,0.85,0.3"
out_put_table="${appdb}.label_l2_result_scoring_di"
test_flag="0"
whitelist="dim_sdk_mapping.whitelist_dpiage_shandong_used"

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.AgeScoreV3 \
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
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$modelPath0" "$modelPath1" "$modelPath2" "$threshold" "$whitelist" "$out_put_table" "$day" $test_flag
