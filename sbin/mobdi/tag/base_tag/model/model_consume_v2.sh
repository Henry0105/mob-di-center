#!/bin/bash
set -e -x
: '
@owner:guanyt
@describe:设备的consume_level预测
@projectName:mobdi
@BusinessName:profile_model
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


#seed="select device,applist from test.zhangxy_consume_device_sample2"
seed="select device,applist from ${label_l1_applist_refine_cnt_di} where day='$day'"
#mapping="dim_sdk_mapping.mapping_consume_pkg"
mapping=$dim_mapping_consume_pkg
output_table=${label_l1_consume_1001_di}
model_path="/dmgroup/dba/modelpath/20200721/consume_1001/cluster_6"
model_path2="/dmgroup/dba/modelpath/20200721/consume_1001/cluster_9"
tran_flag=0

hive -v -e "
CREATE TABLE  if not exists $output_table (
  device  string,
   bank  bigint,
   beauty  bigint,
   beauty_borrow  bigint,
   borrow  bigint,
   card  bigint,
   fans  bigint,
   haitao  bigint,
   highend_shop  bigint,
   legend_game  bigint,
   luxury_car  bigint,
   makeup  bigint,
   money_game  bigint,
   photo  bigint,
   shopping  bigint,
   prediction_only_install  int,
   cluster4_prediction_nooffline  int,
   cluster  int)
COMMENT '聚类结果表'
partitioned by
(day string)
stored as orc;
"


spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.ConsumeV2 \
--driver-memory 8G \
--executor-memory 15G \
--executor-cores 5 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/mobdi_center/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$day" "$seed" "$mapping" "$output_table" "$model_path" "$model_path2" $tran_flag

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.support.quoted.identifiers=None;

insert overwrite table $output_table partition (day='$day')
select \`(cluster|day)?+.+\`,
case
  when prediction_only_install = 4 then 1 --超高消费（典型场景：豪车，高端商场）
  when prediction_only_install in (1, 2) then 2 --高消费（典型场景：美容、化妆、追星、海淘）
  when prediction_only_install = 5 then 3 --中高消费（典型场景：借贷、刷卡、氪金游戏）
  when prediction_only_install = 3 then 4 --中消费（无典型场景，各类都比较均衡，相对来说刷卡比较多）
end as cluster
from $output_table
where day='$day' and prediction_only_install <> 0

union all

select \`(cluster|day)?+.+\`,
 -1 as cluster --未知，未安装任何与消费相关的app
from $output_table
where day='$day' and  prediction_only_install = 0 and cluster4_prediction_nooffline is null

union all

select \`(cluster|day)?+.+\`,
case
  when cluster4_prediction_nooffline in (1, 2, 8) then 5 --低消费（在任务消费场景app及线下场景下都非常低）
  when cluster4_prediction_nooffline in (4, 3, 5) then 3 --中高消费（典型场景：借贷、刷卡、氪金游戏）
  when cluster4_prediction_nooffline = 0 then 2 --高消费（典型场景：化妆、追星）
  when cluster4_prediction_nooffline in  (6, 7) then 4 --中消费（借贷类比较多）
end as cluster
from $output_table
where day='$day' and prediction_only_install = 0 and  cluster4_prediction_nooffline is not null

"