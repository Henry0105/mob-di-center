#!/bin/bash

set -e -x

: '
   此版数据是为了获取客户信任，调整参数以获取较均匀、数值较高的置信度，置信度无实际参考价值
   生成置信度全量表
   输出表:rp_mobdi_app.device_models_confidence_full_customer,rp_mobdi_app.device_models_confidence_full_customer_view
   author:qinff
   处理流程：
   1.从dw_mobdi_md.result_scoring_par取数计算增量数据的置信度 与 rp_mobdi_app.device_models_confidence_full 关联替换增量置信度
   2.视图rp_mobdi_app.device_models_confidence_full_customer_view指向rp_mobdi_app.device_models_confidence_full_customer的最新分区数据
   3.删除rp_mobdi_app.device_models_confidence_full_customer过期分区, 只保留最近五个分区
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"
#device_models_confidence_full_customer=dm_mobdi_report.device_models_confidence_full_customer
#device_models_confidence_full_customer_view=dm_mobdi_report.device_models_confidence_full_customer_view
#label_l2_result_scoring_di=dm_mobdi_report.label_l2_result_scoring_di
#device_models_confidence_full=dm_mobdi_report.device_models_confidence_full

#model_confidence_config_maping_customer=tp_mobdi_model.model_confidence_config_maping_customer
#model_confidence_config_maping_customer=dim_sdk_mapping.model_confidence_config_maping_customer

#全量自洽表对外提供，类似full表，保留最近5个分区即可。
#dw_mobdi_md.models_with_confidence_union_logic_par,dw_mobdi_md.device_cate_preference_incr->rp_mobdi_app.device_models_confidence_full,rp_mobdi_app.device_models_confidence_full_view
new_ver=${day}.1000

lastPartStr=`hive -e "show partitions $device_models_confidence_full_customer" | sort | tail -n 1`

if [ -z "$lastPartStr" ]; then
    lastPartStrA=$lastPartStr
fi

if [ -n "$lastPartStr" ]; then
    lastPartStrA=" AND  $lastPartStr"
fi

echo $lastPartStrA


hive -v -e"
insert overwrite table $device_models_confidence_full_customer partition(version='$new_ver')
select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
       car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
       industry_cl,agebin_1001,agebin_1001_cl,processtime,coalesce(cate_preference_list,''),income_1001,income_1001_cl,
       occupation_1001,occupation_1001_cl
from
(
  select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
         car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
         industry_cl,agebin_1001,agebin_1001_cl,processtime,cate_preference_list,income_1001,income_1001_cl,
         occupation_1001,occupation_1001_cl,
         row_number() over(partition by device order by processtime desc) rn
  from
  (
    select confidence.device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
           car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
           industry_cl,agebin_1001,agebin_1001_cl,processtime,cate_preference_list,
           income_1001,income_1001_cl,occupation_1001,occupation_1001_cl
    from (select device,
                 max(gender_cl) as gender_cl,
                 max(agebin_cl) as agebin_cl,
                 max(edu_cl) as edu_cl,
                 max(income_cl) as income_cl,
                 max(kids_cl) as kids_cl,
                 max(car_cl) as car_cl,
                 max(house_cl) as house_cl,
                 max(married_cl) as married_cl,
                 max(occupation_cl) as occupation_cl,
                 max(industry_cl) as industry_cl,
                 max(agebin_1001_cl) as agebin_1001_cl,
                 max(income_1001_cl) as income_1001_cl,
                 max(occupation_1001_cl) as occupation_1001_cl
          from (select device,
                       case when kind='gender' then confidence else 0 end as gender_cl,
                       case when kind='agebin' then confidence else 0 end as agebin_cl,
                       case when kind='edu' then confidence else 0 end as edu_cl,
                       case when kind='income' then confidence else 0 end as income_cl,
                       case when kind='kids' then confidence else 0 end as kids_cl,
                       case when kind='car' then confidence else 0 end as car_cl,
                       case when kind='house' then confidence else 0 end as house_cl,
                       case when kind='married' then confidence else 0 end as married_cl,
                       case when kind='occupation' then confidence else 0 end as occupation_cl,
                       case when kind='industry' then confidence else 0 end as industry_cl,
                       case when kind='agebin_1001' then confidence else 0 end as agebin_1001_cl,
                       case when kind='income_1001' then confidence else 0 end as income_1001_cl,
                       case when kind='occupation_1001' then confidence else 0 end as occupation_1001_cl
                 from (select device,prediction,probability,day,kind,
                              case
                                when (quadratic_coefficient*pow(normal_probability,2)+primary_coefficient*normal_probability+intercept) > 0.999 then 0.999
                                when (quadratic_coefficient*pow(normal_probability,2)+primary_coefficient*normal_probability+intercept) < 0 then 0.0
                                else (quadratic_coefficient*pow(normal_probability,2)+primary_coefficient*normal_probability+intercept)
                              end as confidence
                        from (select device,a1.prediction,probability,day,a1.kind,
                                      case
                                        when probability > max_probability then probability
                                        when probability <= max_probability and probability >= min_probability
                                        then (probability-min_probability)/(max_probability-min_probability)
                                        else 0.0
                                      end as normal_probability,
                                     quadratic_coefficient,primary_coefficient,intercept
                              from ( select *
                                     from $model_confidence_config_maping_customer
                                     where version='1006') a1
                              inner join (select device,prediction,probability,day,kind
                                          from $label_l2_result_scoring_di
                                          where day='$day') a2
                              on a1.kind = a2.kind
                              and a1.prediction = a2.prediction) t1
                        ) t2
                ) a
          group by device) confidence
    left join (select device,gender,agebin,edu,income,
                      kids,car,house,married,occupation,
                      industry,agebin_1001,processtime,cate_preference_list,income_1001,occupation_1001
               from $device_models_confidence_full
               where version = '$new_ver') full
    on confidence.device=full.device

    union all

    select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
           car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
           industry_cl,agebin_1001,agebin_1001_cl,processtime,cate_preference_list,income_1001,income_1001_cl,
           occupation_1001,occupation_1001_cl
    from $device_models_confidence_full_customer
    where 1=1 ${lastPartStrA}
  ) un
) tmp
where rn=1;
"

#实现删除过期的分区的功能，只保留最近5个分区
for old_version in `hive -e "show partitions $device_models_confidence_full_customer " | grep -v '_bak' | sort | head -n -10`
do
    echo "rm $old_version"
    hive -v -e "alter table $device_models_confidence_full_customer drop if exists partition($old_version)"
done


hive -v -e "
create or replace view $device_models_confidence_full_customer_view as
select *
from $device_models_confidence_full_customer
where version='${new_ver}';
"

echo "run over of $day"
