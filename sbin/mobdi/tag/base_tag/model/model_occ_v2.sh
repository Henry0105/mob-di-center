#!/bin/bash
set -e -x
: '
@owner:yeyy
@describe:设备的职业预测v2
@projectName:mobdi
@BusinessName:model_occ
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

tmpdb="dw_mobdi_md"
appdb="rp_mobdi_app"
## input
label_merge_all="dw_mobdi_md.model_merge_all_features"
label_apppkg_feature_index="${label_l1_apppkg_feature_index}"
label_apppkg_category_index="${label_l1_apppkg_category_index}"

## out
tmp_occ_predict=dw_mobdi_md.tmp_occ_predict


# 由于不能并行，故用model去启动特征计算
sh /home/dba/mobdi/tag/base_tag/label/l1/label_occ_v2_features.sh $day

HADOOP_USER_NAME=dba hive -v -e "
drop table if exists  ${tmpdb}.tmp_occ_score_part1;
create table  ${tmpdb}.tmp_occ_score_part1 stored as orc as
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device
      ,case
        when city_level_1001 = 1 then 0
        when city_level_1001 = 2  then 1
        when city_level_1001 = 3  then 2
        when city_level_1001 = 4  then 3
        when city_level_1001 = 5  then 4
        when city_level_1001 = 6  then 5
        else 6 end as index
       ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device
      ,case
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
        when factory = 'LEMOBILE' then 17
        when factory = '360' then 18
        when factory = 'BLACKSHARK' then 19
        else 20 end as index
      ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device
      ,case
        when split(sysver, '\\\\.')[0] = 10 then 21
        when split(sysver, '\\\\.')[0] = 9 then 22
        when split(sysver, '\\\\.')[0] = 8 then 23
        when split(sysver, '\\\\.')[0] = 7 then 24
        when split(sysver, '\\\\.')[0] = 6 then 25
        when split(sysver, '\\\\.')[0] = 5 then 26
        when split(sysver, '\\\\.')[0] <= 4 then 27
        when sysver = 'unknown' then 28
        else 29 end as index
      ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
    when diff_month < 12 then 30
    when diff_month >= 12 and diff_month < 24 then 31
    when diff_month >= 24 and diff_month < 36 then 32
    when diff_month >= 36 and diff_month < 48 then 33
    when diff_month >= 48 then 34
  else 35
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
  when tot_install_apps <= 10 then 36
  when tot_install_apps <= 20 then 37
  when tot_install_apps <= 30 then 38
  when tot_install_apps <= 50 then 39
  when tot_install_apps <= 100 then 40
  else 41
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all
  select device,
  case
  when price > 0 and price < 1000 then 42
  when price >= 1000 and price < 1499 then 43
  when price >= 1499 and price < 2399 then 44
  when price >= 2399 and price < 4000 then 45
  when price >= 4000 then 46
  else 47
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
    when house_price >= 0 and house_price < 8000 then 48
    when house_price >= 8000 and house_price < 12000 then 49
    when house_price >= 12000 and house_price < 22000 then 50
    when house_price >= 22000 and house_price < 40000 then 51
    when house_price >= 40000 and house_price < 60000 then 52
    when house_price >= 60000 then 53
    else 54
    end as index,
    1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.age.cate_l1'

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.age.cate_l2'


)t group by device
"

HADOOP_USER_NAME=dba hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

drop table if exists  ${tmpdb}.tmp_occ_score_part3;
create table ${tmpdb}.tmp_occ_score_part3 stored as orc as
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device
        ,case when year>='1995' and year<='1999' then 0
            when year>='2000' and year<='2004' then 1
            when year>='2005' and year<='2009' then 2
            when year>='2010' and year<='2014' then 3
            when year>='2015' and year<='2019' then 4
            else 5 end index
      ,1.0 cnt
  from ${appdb}.label_phone_year

  union all
  select device
        ,case when cnt=1 then 6
              when cnt in (2,3) then 7
              when cnt in (4,5) then 8
              when cnt>5 then 9
              else 10 end index
        ,1.0 cnt
  from ${appdb}.label_bssid_num

  union all
  select device
        ,case when distance < 50 then 11
              when distance < 500 then 12
              when distance < 5000 then 13
              when distance >= 5000 then 14
              else 15 end index
        ,1.0 cnt
  from ${appdb}.label_distance_avg

  union all
  select device
        ,case when distance < 50 then 16
              when distance < 500 then 17
              when distance < 5000 then 18
              when distance >= 5000 then 19
              else 20 end index
        ,1.0 cnt
  from ${appdb}.label_distance_night

  union all
  select device
        ,case when home_work_dist=0 then 21
              when home_work_dist<=1000 then 22
              when home_work_dist<=10000 then 23
              when home_work_dist<=50000 then 24
              when home_work_dist>50000 then 25
              else 26 end index
        ,1.0 cnt
  from ${appdb}.label_homeworkdist

  union all
  select device,cast(26+cast(poi_type as double) as int) index,1.0 cnt
  from ${appdb}.label_home_poiaround

  union all
  select device,cast(46+cast(poi_type as double) as int) index,1.0 cnt
  from ${appdb}.label_work_poiaround

    union all
   select device,index,cnt from ${appdb}.label_occ_score_workpoi_mid
)a
group by device
"


HADOOP_USER_NAME=dba hive -v -e "
drop table if exists  ${tmpdb}.tmp_occ_score_part4;
create  table ${tmpdb}.tmp_occ_score_part4 stored as orc as
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select device, index, 1.0 as cnt from dw_mobdi_md.income_1001_university_bssid_index where day='$day'

  union all

  select device, index, 1.0 as cnt from dw_mobdi_md.income_1001_shopping_mall_bssid_index where day='$day'

  union all

  select device, index, 1.0 as cnt
  from dw_mobdi_md.income_1001_traffic_bssid_index
  LATERAL VIEW explode(Array(traffic_bus_index,traffic_subway_index,traffic_airport_index,traffic_train_index)) a as index
  where day='$day'

  union all

  select device, index, 1.0 as cnt
  from dw_mobdi_md.income_1001_hotel_bssid_index
  LATERAL VIEW explode(Array(price_level1_index,price_level2_index,price_level3_index,price_level4_index,price_level5_index,
                             price_level6_index,rank_star1_index,rank_star2_index,rank_star3_index,rank_star4_index,
                             rank_star5_index,score_type1_index,score_type2_index,score_type3_index)) a as index
  where day='$day'
)a
group by device
"


HADOOP_USER_NAME=dba hive -v -e "
drop table if exists  ${tmpdb}.tmp_occ_score_part2;
create table ${tmpdb}.tmp_occ_score_part2 stored as orc as
select device,index,cnt from
${appdb}.label_occ_score_applist
"

HADOOP_USER_NAME=dba hive -v -e "
drop table if exists  ${tmpdb}.tmp_occ_score_app2vec;
create table ${tmpdb}.tmp_occ_score_app2vec stored as orc as
select * from ${appdb}.label_occ_app2vec
"


## 输入表

part1Table="${tmpdb}.tmp_occ_score_part1"
part2Table="${tmpdb}.tmp_occ_score_part2"
part3Table="${tmpdb}.tmp_occ_score_part3"
part4Table="${tmpdb}.tmp_occ_score_part4"
app2vecTable="${tmpdb}.tmp_occ_score_app2vec"
modelPath="/dmgroup/dba/modelpath/20200811/linear_regression_model/occ_lr"
outputTable="dw_mobdi_md.tmp_occ_predict"

HADOOP_USER_NAME=dba  /opt/mobdata/sbin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.youzu.mob.score.OccupationNewScoring2 \
--driver-memory 8G \
--executor-memory 21G \
--executor-cores 7 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.sql.shuffle.partitions=10000 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=8192 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.speculation=true \
--conf spark.driver.maxResultSize=4g \
--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
/home/mobdi_test/yeyy_dir/shellfile/occ_full/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$modelPath" "$part1Table" "$part2Table" "$part3Table"  "$part4Table"  "$app2vecTable" "$outputTable" "$day"


hive -v -e"
insert overwrite table rp_mobdi_app.label_l2_result_scoring_di partition(day='$day',kind='occupation')
select device,prediction,probability
from
dw_mobdi_md.tmp_occ_predict
where day='$day'
"

