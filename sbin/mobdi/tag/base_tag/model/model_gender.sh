#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:设备的gender预测
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

#input
label_merge_all="${tmpdb}.model_merge_all_features"
label_apppkg_feature_index=${label_l1_apppkg_feature_index}
label_apppkg_category_index=${label_l1_apppkg_category_index}
income_1001_university_bssid_index=${tmpdb}.income_1001_university_bssid_index
income_1001_shopping_mall_bssid_index=${tmpdb}.income_1001_shopping_mall_bssid_index
income_1001_traffic_bssid_index=${tmpdb}.income_1001_traffic_bssid_index
income_1001_hotel_bssid_index=${tmpdb}.income_1001_hotel_bssid_index

income_1001_pid_contacts_index=${tmpdb}.income_1001_pid_contacts_index_sec   # 加密表
income_1001_pid_contacts_word2vec_index=${tmpdb}.income_1001_phone_contacts_word2vec_index  # 需要改表名

modelPath="/dmgroup/dba/modelpath/20200413/gender"
length=7300

#tmp
gender_all_index=${tmpdb}.gender_all_index

#output
outputTable=${label_l2_result_scoring_di}


hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $gender_all_index partition(day='$day')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device,
         case
           when city_level_1001 = 1 then 0
           when city_level_1001 = 2 then 1
           when city_level_1001 = 3 then 2
           when city_level_1001 = 4 then 3
           when city_level_1001 = 5 then 4
           when city_level_1001 = 6 then 5
           else 6
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when factory = 'HUAWEI' then 7
           when factory = 'OPPO' then 8
           when factory = 'VIVO' then 9
           when factory = 'XIAOMI' then 10
           when factory = 'SAMSUNG' then 11
           when factory = 'MEIZU' then 12
           when factory = 'ONEPLUS' then 13
           when factory = 'SMARTISAN' then 14
           when factory = 'GIONEE' then 15
           when factory = 'MEITU' then 16
           else 18
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when split(sysver, '\\\\.')[0] >= 9 then 19
           when split(sysver, '\\\\.')[0] = 8 then 20
           when split(sysver, '\\\\.')[0] = 7 then 21
           when split(sysver, '\\\\.')[0] = 6 then 22
           when split(sysver, '\\\\.')[0] = 5 then 23
           when split(sysver, '\\\\.')[0] <= 4 then 24
           when sysver = 'unknown' then 25
           else 26
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when diff_month < 12 then 27
           when diff_month >= 12 and diff_month < 24 then 28
           when diff_month >= 24 and diff_month < 36 then 29
           when diff_month >= 36 then 30
           else 31
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when tot_install_apps <= 10 then 32
           when tot_install_apps > 10 and tot_install_apps <= 20 then 33
           when tot_install_apps > 20 and tot_install_apps <= 30 then 34
           when tot_install_apps > 30 and tot_install_apps <= 50 then 35
           when tot_install_apps > 50 and tot_install_apps <= 100 then 36
           else 37
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when price > 0 and price < 1000 then 38
           when price >= 1000 and price < 1499 then 39
           when price >= 1499 and price < 2399 then 40
           when price >= 2399 and price < 4000 then 41
           when price >= 4000 then 42
           else 43
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
         case
           when house_price >= 0 and house_price < 8000 then 44
           when house_price >= 8000 and house_price < 12000 then 45
           when house_price >= 12000 and house_price < 22000 then 46
           when house_price >= 22000 and house_price < 40000 then 47
           when house_price >= 40000 and house_price < 50000 then 48
           else 49
         end as index,
         1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,index,cnt from $label_apppkg_feature_index where day = '${day}' and version = '1003_gender'

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.gender.cate_l1'

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.gender.cate_l2'

  union all

  select device, index+5000 as index, 1.0 as cnt from $income_1001_university_bssid_index where day='$day'

  union all

  select device, index+5000 as index, 1.0 as cnt from $income_1001_shopping_mall_bssid_index where day='$day'

  union all

  select device, index+5000 as index, 1.0 as cnt
  from $income_1001_traffic_bssid_index
  LATERAL VIEW explode(Array(traffic_bus_index,traffic_subway_index,traffic_airport_index,traffic_train_index)) a as index
  where day='$day'

  union all

  select device, index+5000 as index, 1.0 as cnt
  from $income_1001_hotel_bssid_index
  LATERAL VIEW explode(Array(price_level1_index,price_level2_index,price_level3_index,price_level4_index,price_level5_index,
                             price_level6_index,rank_star1_index,rank_star2_index,rank_star3_index,rank_star4_index,
                             rank_star5_index,score_type1_index,score_type2_index,score_type3_index)) a as index
  where day='$day'

  union all

  select device, index-14400 as index, 1.0 as cnt
  from $income_1001_pid_contacts_index
  LATERAL VIEW explode(Array(micro_business_flag_index,score_level_index,if_company_index,company_size_index,company_rk_index)) a as index
  where day='$day'

  union all

  select device, n.word+5100 as index, 1.0 as cnt
  from $income_1001_pid_contacts_index
  lateral view explode(word_index) n as word
  where day='$day'

  union all

  select device, index-14396 as index, 1.0 as cnt from $income_1001_pid_contacts_word2vec_index where day='$day'
) as aa
group by device;
"

tmp_sql="
select device,index,cnt
from $gender_all_index
where day='$day'
"

model_app2vec=${label_l1_device_model_app2vec}

spark2-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance2 \
--class com.youzu.mob.newscore.GenderScore \
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
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$tmp_sql" "$length" "$outputTable" "$day" "$model_app2vec"
