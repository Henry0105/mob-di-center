#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:设备的occupation预测
@projectName:mobdi
@BusinessName:profile_model
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi
day=$1

##input
transfered_feature_table="dw_mobdi_md.model_transfered_features"
label_apppkg_feature_index=${label_l1_apppkg_feature_index}

modelPath="/dmgroup/dba/modelpath/20190815/linear_regression_model/occupationmodel"
length=330000
threshold="2.0,0.5,1.0,1.0,1.0,0.2,0.5,1"

model_index="tp_mobdi_model.model_index"
##output
outputTable=${label_l2_result_scoring_di}

tmp_sql="
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
  $model_index t on version = '1003' and model = 'occupation' and t.index=t1.index
  group by device, t1.index, cnt
) t2
group by device
"

spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.OccupationScore \
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
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$tmp_sql" "$length" "$outputTable" "$day" "$threshold"
