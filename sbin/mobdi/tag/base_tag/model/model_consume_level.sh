#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
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

#input
transfered_feature_table="dw_mobdi_md.model_transfered_features"
label_merge_all="dw_mobdi_md.model_merge_all_features"
label_apppkg_feature_index=${label_l1_apppkg_feature_index}
label_apppkg_category_index=${label_l1_apppkg_category_index}

model="/dmgroup/dba/modelpath/20200413/consume_level"
out_put_table=rp_mobdi_app.label_l2_result_scoring_di
threshold="1.0,1.1,1.0,1.0"

#先复用已经生成的dw_mobdi_md.device_info_level_par_new表的设备特征
#其中cell_factory特征(8-15)、sysver特征(16-22)、price特征(33-37)与device_info_level_par_new一样，直接复用
#城市等级特征用的是city_level_1001，和device_info_level_par_new不一样，所以要用逻辑获取
#加上头部app特征索引
#加上头部app安装数量特征索引
#加上头部app分类特征索引
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table dw_mobdi_md.consume_level_device_index partition(day='$day')
select device,index,1.0 as cnt
from $transfered_feature_table
where day='$day'
and (
  (index>=8 and index<=22)
  or (index>=33 and index<=37)
)

union all

select device,
       case
         when city_level_1001 = -1 or city_level_1001 is null then 0
         when city_level_1001 = 1 then 1
         when city_level_1001 = 2 then 2
         when city_level_1001 = 3 then 3
         when city_level_1001 = 4 then 4
         when city_level_1001 = 5 then 5
         when city_level_1001 = 6 then 6
         else 7
       end as index,
       1.0 as cnt
from $label_merge_all
where day = '$day'

union all

select device,index,1.0 as cnt
from $label_apppkg_feature_index
where day = '$day'
and version = '1003_common'

union all

select device,
       case
         when tot_install_apps <= 16 then 28
         when tot_install_apps > 16 and tot_install_apps <= 25 then 29
         when tot_install_apps > 26 and tot_install_apps <= 37 then 30
         when tot_install_apps > 37 and tot_install_apps <= 52 then 31
         else 32
       end as index,
       1.0 as cnt
from
(
  select t1.device, count(1) as tot_install_apps
  from
  (
    select device, pkg
    from dm_mobdi_mapping.device_applist_new
    where day = '$day'
  ) t1
  inner join
  tp_mobdi_model.apppkg_index t2 on t1.pkg=t2.apppkg and t2.model='common' and t2.version='1003'
  group by t1.device
) t

union all

select device,index,cnt
from $label_apppkg_category_index
where day = '$day'
and version = '1003.consume_level';
"

#为了适配分析师的onehot算法，需要对consume_level_device_index表进行group by处理
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table dw_mobdi_md.consume_level_device_index_onehot_prepare partition(day='$day')
select device,
       cast(sum(if(index>=0 and index<=7,index,0)) as int) as city_level_1001,
       cast(sum(if(index>=8 and index<=15,index,0)) as int) as factory,
       cast(sum(if(index>=16 and index<=22,index,0)) as int) as sysver,
       if(cast(sum(if(index>=28 and index<=32,index,0)) as int)=0,
         28,
         cast(sum(if(index>=28 and index<=32,index,0)) as int)
       ) as app_cnt,
       cast(sum(if(index>=33 and index<=37,index,0)) as int) as price,
       if(size(collect_list(if(index>=101 and index<1001,index,null)))=0,
         collect_set(0),
         collect_list(if(index>=101 and index<1001,index,null))) as cate_index_list,
       if(size(collect_list(if(index>=101 and index<1001,cnt,null)))=0,
         collect_set(0.0),
         collect_list(if(index>=101 and index<1001,cnt,null))) as cate_cnt_list,
       if(size(collect_list(if(index>=1001,index,null)))=0,
         collect_set(0),
         collect_list(if(index>=1001,index,null))) as apppkg_index_list
from dw_mobdi_md.consume_level_device_index
where day = '$day'
group by device;
"

pre_sql="
select device,city_level_1001,factory,sysver,app_cnt,price,cate_index_list,cate_cnt_list,apppkg_index_list
from dw_mobdi_md.consume_level_device_index_onehot_prepare
where day='$day'
"

spark2-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.newscore.ConsumeLevelScore \
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
/home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$model" "$pre_sql" "$threshold" "$out_put_table" "$day"
