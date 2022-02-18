#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:设备的income_1001预测
@projectName:mobdi
@BusinessName:profile_model
@attention: income_1001的依赖的所有tmp表，目前还是用的老的,后面需要把这些md的表都固化为标签
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

tmpdb=${dm_mobdi_tmp}

#input
transfered_feature_table="${tmpdb}.model_transfered_features"
label_merge_all="${tmpdb}.model_merge_all_features"
label_apppkg_feature_index=${label_l1_apppkg_feature_index}
label_apppkg_category_index=${label_l1_apppkg_category_index}

income_1001_university_bssid_index=${tmpdb}.income_1001_university_bssid_index
income_1001_shopping_mall_bssid_index=${tmpdb}.income_1001_shopping_mall_bssid_index
income_1001_traffic_bssid_index=${tmpdb}.income_1001_traffic_bssid_index
income_1001_hotel_bssid_index=${tmpdb}.income_1001_hotel_bssid_index

income_1001_pid_contacts_index=${tmpdb}.income_1001_pid_contacts_index_sec   # 加密表
income_1001_pid_contacts_word2vec_index=${tmpdb}.income_1001_phone_contacts_word2vec_index  # 需要改表名


modelPath="/dmgroup/dba/modelpath/20191128/income_1001/incomemodel_lr_201911_new"
threshold="0.9,1.2,1.15,1.0,0.6"
length=21696

#tmp
income_1001_device_index="${tmpdb}.income_1001_device_index"
income_1001_all_index="${tmpdb}.income_1001_all_index"
#output
outputTable=${label_l2_result_scoring_di}

#直接复用已经生成的dw_mobdi_md.device_info_level_par_new表的设备特征，去计算income_1001的设备特征
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $income_1001_device_index partition(day='$day')
select device,
       case
         when index>=0 and index<=6 then index+46
         when index>=8 and index<=15 then index+45
         when index>=16 and index<=22 then index+45
         when index>=23 and index<=27 then index+45
         when index>=28 and index<=32 then index+45
         when index>=33 and index<=37 then index+45
       end as index
from $transfered_feature_table
where day='$day'
and index>=0
and index<=37

union all

select device,
       case
         when house_price >= 0 and house_price < 8000 then 83
         when house_price >= 8000 and house_price < 12000 then 84
         when house_price >= 12000 and house_price < 22000 then 85
         when house_price >= 22000 and house_price < 40000 then 86
         when house_price >= 40000 and house_price < 50000 then 87
         when house_price >= 50000 then 88
         else 89
       end as index
from $label_merge_all
where day='$day';
"

#合并所有特征
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $income_1001_all_index partition(day='$day')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device, index, 1.0 as cnt from $income_1001_university_bssid_index where day='$day'
  union all
  select device, index, 1.0 as cnt from $income_1001_shopping_mall_bssid_index where day='$day'
  union all
  select device, index, 1.0 as cnt from $income_1001_traffic_bssid_index
  LATERAL VIEW explode(Array(traffic_bus_index,traffic_subway_index,traffic_airport_index,traffic_train_index)) a as index
  where day='$day'
  union all
  select device, index, 1.0 as cnt from $income_1001_hotel_bssid_index
  LATERAL VIEW explode(Array(price_level1_index,price_level2_index,price_level3_index,price_level4_index,price_level5_index,
                             price_level6_index,rank_star1_index,rank_star2_index,rank_star3_index,rank_star4_index,
                             rank_star5_index,score_type1_index,score_type2_index,score_type3_index)) a as index
  where day='$day'
  union all
  select device, index, 1.0 as cnt from $income_1001_device_index where day='$day'
  union all
  select device, index, 1.0 as cnt from $label_apppkg_feature_index
  where day='$day' and version = '1003_income1001'
  union all
  select device, index, 1.0 as cnt from $label_apppkg_category_index
  where day='$day' and version='1003.income_1001'
  union all
  select device, index, 1.0 as cnt from $income_1001_pid_contacts_index
  LATERAL VIEW explode(Array(micro_business_flag_index,score_level_index,if_company_index,company_size_index,company_rk_index)) a as index
  where day='$day'
  union all
  select device, n.word+19496 as index, 1.0 as cnt
  from $income_1001_pid_contacts_index
  lateral view explode(word_index) n as word
  where day='$day'
  union all
  select device, index, 1.0 as cnt from $income_1001_pid_contacts_word2vec_index where day='$day'
) a
group by device;
"

tmp_sql="
select device,index,cnt
from $income_1001_all_index
where day='$day'
"

spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.Income1001Score \
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
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$tmp_sql" "$threshold" "$length" "$outputTable" "$day"
