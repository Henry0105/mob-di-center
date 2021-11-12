#!/bin/sh
set -e -x
: '
@owner:xdzhang
@describe:模型自洽标签字段合成到同一张表中去
@projectName:MobDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=${dm_mobdi_tmp}
models_with_confidence_pre=${tmpdb}.models_with_confidence_pre

## input
result_scoring_par=$label_l2_result_scoring_di

##中间落地表
models_with_confidence_pre_par=${tmpdb}.models_with_confidence_pre_par

## mapping
#model_confidence_config_maping=tp_mobdi_model.model_confidence_config_maping

##output 添加了置信度并做了逻辑自洽的表
confidence_logic=$label_l2_model_with_confidence_union_logic_di

day=$1
echo ${day}

#step 1: 计算置模型标签值及值的置信度
last_conf_par=`hive -e "show partitions tp_mobdi_model.model_confidence_config_maping" | sort | tail -n 1`

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $models_with_confidence_pre
select device,prediction,probability,day,kind,
       case
         when normal_probability = 1 then 0.9
         when (quadratic_coefficient*pow(normal_probability,2)+primary_coefficient*normal_probability+intercept) > 1 then 1.0
         when (quadratic_coefficient*pow(normal_probability,2)+primary_coefficient*normal_probability+intercept) < 0 then 0.0
         else (quadratic_coefficient*pow(normal_probability,2)+primary_coefficient*normal_probability+intercept)
       end as confidence
from
(
  select device,a1.prediction,probability,day,a1.kind,
         case
           when probability > max_probability then probability
           when probability <= max_probability and probability >= min_probability
           then (probability-min_probability)/(max_probability-min_probability)
           else 0.0
         end as normal_probability,
         quadratic_coefficient,primary_coefficient,intercept
  from
  (
    select *
    from $model_confidence_config_maping
    where $last_conf_par
    and kind != 'consume_level'
  ) a1
  inner join
  (
    select device,prediction,probability,day,kind
    from $result_scoring_par
    where day='$day'
    and kind != 'consume_level'
  ) a2 on a1.kind = a2.kind and a1.prediction = a2.prediction
) a

union all

select b.device,b.prediction,b.probability,'$day' as day,'consume_level' as kind,
       case
         when b.probability >= a.max_prob then a.max_prob
         when b.probability < a.max_prob and b.probability >= a.min_prob then (b.probability-a.min_prob)/(a.max_prob-a.min_prob)
         else 0.0
       end as confidence
from
(
  select prediction,max_probability as max_prob,min_probability as min_prob
  from $model_confidence_config_maping
  where $last_conf_par
  and kind = 'consume_level'
)a
inner join
(
  select device,prediction,probability
  from $result_scoring_par
  where day='$day'
  and kind='consume_level'
) b on a.prediction=b.prediction
;

insert overwrite table $models_with_confidence_pre_par partition(day='$day')
select device,prediction,probability,kind,confidence
from dw_mobdi_md.models_with_confidence_pre;
"

#增加edu的前置逻辑自洽
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $models_with_confidence_pre_par partition(day = '$day')
select a.device,
       case when b.prediction=6 and a.prediction=9 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.8 then 8
       when b.prediction=6 and a.prediction=8 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.3 then 7
       when b.prediction=6 and a.prediction=7 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 6
       when a.prediction=9 and b.prediction=5 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.5 then 8
       when a.prediction=9 and b.prediction=4 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 8
       when a.prediction=8 and b.prediction=3 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 9
       when a.prediction=8 and b.prediction=2 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 9
       when a.prediction=8 and b.prediction=1 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then 9
       else a.prediction end prediction,
       a.probability,
       a.kind,
       case when b.prediction=6 and a.prediction=9 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.8 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when b.prediction=6 and a.prediction=8 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.3 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when b.prediction=6 and a.prediction=7 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=9 and b.prediction=5 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.5 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=9 and b.prediction=4 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=8 and b.prediction=3 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=8 and b.prediction=2 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       when a.prediction=8 and b.prediction=1 and conv(substr(a.device, 0, 4), 16 ,10)/65535<0.1 then if(a.confidence/b.probability>1,b.probability,a.confidence)
       else a.confidence end confidence
from
(
    select device,prediction,probability,kind,confidence
    from $models_with_confidence_pre_par
    where day = '$day'
    and kind = 'edu'
)a
left join
(
    select device,prediction,probability,kind
    from $result_scoring_par
    where day = '$day'
    and kind = 'city_level'
)b
on a.device = b.device

union all

select device,prediction,probability,kind,confidence
from $models_with_confidence_pre_par
where day = '$day'
and kind <> 'edu';
"

# step2 使用通用工具进行逻辑自洽和置信度计算
path=$(dirname "$0")
spark2-submit --class com.youzu.mob.newscore.ModelProfileTableMerge \
  --queue root.yarn_data_compliance2 \
  --master yarn-cluster \
  --name merge_two_$day \
  --driver-memory 4G \
  --executor-memory 12G \
  --executor-cores 4 \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=100 \
  --conf spark.dynamicAllocation.maxExecutors=200 \
  --files ${path}/tag_merge_2.properties \
  --conf spark.sql.shuffle.partitions=800 \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar ${day} ${path}/tag_merge_2.properties

#全字段去重
hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $label_l2_model_with_confidence_union_logic_di partition(day=$day)
select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
       car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
       industry_cl,agebin_1001,agebin_1001_cl,city_level,special_time,consum_level,
       life_stage,income_1001,income_1001_cl,occupation_1001,occupation_1001_cl,consume_level,
       consume_level_cl,agebin_1002,agebin_1002_cl,agebin_1003,agebin_1003_cl,income_1001_v2,
       income_1001_v2_cl,occupation_1002,occupation_1002_cl
from $label_l2_model_with_confidence_union_logic_di
where day = '$day'
and gender <> -1
and edu <> -1
group by device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
       car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
       industry_cl,agebin_1001,agebin_1001_cl,city_level,special_time,consum_level,
       life_stage,income_1001,income_1001_cl,occupation_1001,occupation_1001_cl,consume_level,
       consume_level_cl,agebin_1002,agebin_1002_cl,agebin_1003,agebin_1003_cl,income_1001_v2,
       income_1001_v2_cl,occupation_1002,occupation_1002_cl;
"