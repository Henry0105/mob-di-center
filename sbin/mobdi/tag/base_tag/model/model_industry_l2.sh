#!/bin/bash

set -e -x

: '
@owner:guanyt
@describe:重构的industry模型训练
@projectName:mobdi
@BusinessName:profile_model
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> "
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dw_mobdi_tmp

## input
transfered_feature_table="${tmpdb}.model_transfered_features"
label_apppkg_feature_index=${label_l1_apppkg_feature_index}

model_occupation="$label_l2_result_scoring_di" #需要occupation跑出来先
model="/dmgroup/dba/modelpath/20190815/linear_regression_model/industrymodel_whitecollar;/dmgroup/dba/modelpath/20190815/linear_regression_model/industrymodel_service;/dmgroup/dba/modelpath/20190815/linear_regression_model/industrymodel_bluecollar;/dmgroup/dba/modelpath/20190815/linear_regression_model/industrymodel_individual"
length=330000
thresholds="
0.2,5.0,0.5,0.5,5.0,5.0;
0.5,0.5,5.0,0.5,0.5,0.5,0.5,0.3,0.5;
0.5,0.5,5.0,0.5,0.5,0.5,0.5,0.5,0.5,0.5;
0.5,0.5,5.0,0.5,0.5,0.5,0.5,0.5"

#model_index="tp_mobdi_model.model_index"
## output
outputTable=${label_l2_result_scoring_di}

:<<!
@part_1:
实现功能: 对device进行industry的模型计算
实现步骤: 1.将模型数据分别取occupation=13, 17, 18, 19, 输入到jar包中进行模型训练, 结果输出到kind='industry'分区
!

create_industry_sql(){

industry="$1"
occupation_prediction=$2
echo "
select a1.device, a1.index, a1.cnt
from
(
  select device,
         if(size(collect_list(index)) = 0, collect_set(0), collect_list(index)) as index,
         if(size(collect_list(cnt)) = 0, collect_set(cast(0 as double)), collect_list(cnt)) as cnt
  from
  (
    select device, t1.index, cnt
    from
    (
      select device, index, cnt
      from $transfered_feature_table
      where day='$day'

      union all

      select device,index,cnt
      from $label_apppkg_feature_index
      where day = ${day}
      and version = '1003_common'
    ) t1
    inner join
    $model_index t on t.version = '1003' and t.model = '$industry' and t.index=t1.index
    inner join
    $model_index tt on tt.version = '1003' and tt.model = 'occupation' and tt.index=t1.index
    group by device, t1.index, cnt
  ) t2
  group by device
) as a1
inner join
(
  select device
  from $model_occupation
  where day = ${day}
  and kind = 'occupation'
  and prediction=$occupation_prediction
) as a3 on a1.device = a3.device
"
}

pre_sqls="
`create_industry_sql 'industry_whitecollar' 13`;
`create_industry_sql 'industry_service' 17`;
`create_industry_sql 'industry_bluecollar' 18`;
`create_industry_sql 'industry_individual' 19`"

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.IndustryScore \
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
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$model" "$thresholds" "$pre_sqls" "$length" "$outputTable" "$day"
