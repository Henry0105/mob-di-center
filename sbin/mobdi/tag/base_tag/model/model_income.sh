#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:设备的income预测
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

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"

##input
transfered_feature_table="${tmpdb}.model_transfered_features"
label_apppkg_feature_index=${label_l1_apppkg_feature_index}

modelPath="/dmgroup/dba/modelpath/20190815/linear_regression_model/incomemodel"
threshold="1.0,1.1,1.2,1.0,0.6"
length=260000

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
    and version = '1003_income'
  ) t1
  inner join
  $model_index t on version = '1003' and model = 'income' and t.index=t1.index
  group by device, t1.index, cnt
) t2
group by device
"

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.IncomeScore \
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
--conf spark.driver.maxResultSize=4g \
--conf spark.speculation=true \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$tmp_sql" "$threshold" "$length" "$outputTable" "$day"
