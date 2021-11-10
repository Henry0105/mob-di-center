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

threshold="2.4,1.1,0.5,0.45,0.45"
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


tmp_income_1001_all_probability="${tmpdb}.tmp_income_1001_all_probability"
#ads_device_model_income1001_probability_full="dm_mobdi_report.ads_device_model_income1001_probability_full"
#每日所有probability数据汇总
hive -v -e "
set mapreduce.map.memory.mb=15360;
set mapreduce.map.java.opts=-Xmx13800m;
set mapreduce.child.map.java.opts='-Xmx13800m';
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

INSERT OVERWRITE TABLE $ads_device_model_income1001_probability_full PARTITION(day = '$day')
SELECT COALESCE(b.device,a.device) AS device,
       COALESCE(b.probability,a.probability) AS probability
FROM
(
  SELECT device
       , probability
  FROM $ads_device_model_income1001_probability_full
  WHERE day = '$p1day'
) AS a
FULL JOIN
(
  SELECT device
       , probability
  FROM $tmp_income_1001_all_probability
  WHERE day = '$day'
) AS b
ON a.device = b.device;
"

#小文件多，tmp表只保留最近7个分区
for old_version in `hive -e "show partitions ${tmp_income_1001_all_probability} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${tmp_income_1001_all_probability} drop if exists partition($old_version)"
done

for old_version in `hive -e "show partitions ${ads_device_model_income1001_probability_full} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${ads_device_model_income1001_probability_full} drop if exists partition($old_version)"
done