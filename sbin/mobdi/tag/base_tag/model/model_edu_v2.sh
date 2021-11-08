#!/bin/bash
set -e -x
: '
@owner:luost
@describe:设备的edu预测v2
@projectName:mobdi
@BusinessName:model_edu_v2
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
tmp_score_part2="${tmpdb}.tmp_edu_score_part2"
tmp_score_part3="${tmpdb}.tmp_score_part3"
tmp_score_part4="${tmpdb}.tmp_score_part4"
tmp_score_part5="${tmpdb}.tmp_score_part5_v3"
tmp_score_part6="${tmpdb}.tmp_score_part6_v3"
tmp_score_part7="${tmpdb}.tmp_edu_score_part7"
tmp_score_part8="${tmpdb}.tmp_edu_score_part8"
tmp_score_app2vec="${tmpdb}.tmp_score_app2vec"

#参数
modelPath="/dmgroup/dba/modelpath/20200111/linear_regression_model/edu_lr"
threshold="1.7,0.37,0.6,1.35"
out_put_table=$label_l2_result_scoring_di

#准备sql
sql1="
select device,
       index,
       cnt
from $tmp_score_part1
where day = '$day'
"

sql2="
select device,
       index,
       cnt
from $tmp_score_part2
where day = '$day'
"

sql3="
select device,
       index,
       cnt
from $tmp_score_part3
where day = '$day'
"

sql4="
select device,
       index,
       cnt
from $tmp_score_part4
where day = '$day'
"

sql5="
select device,
       index,
       cnt
from $tmp_score_part5
where day = '$day'
"

sql6="
select device,
       index,
       cnt
from $tmp_score_part6
where day = '$day'
"

sql7="
select device,
       index,
       cnt
from $tmp_score_part7
where day = '$day'
"

sql8="
select device,
       index,
       cnt
from $tmp_score_part8
where day = '$day'
"

sql9="
select *
from $tmp_score_app2vec
where day = '$day'
"

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.EduScoreV2 \
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
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$day" "$modelPath" "$threshold" "$sql1" "$sql2" "$sql3" "$sql4" "$sql5" "$sql6" "$sql7" "$sql8" "$sql9" "$out_put_table"

