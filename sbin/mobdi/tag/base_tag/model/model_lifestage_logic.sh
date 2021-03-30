#! /bin/sh
set -e -x

day="$1"

:<<!
实现功能: life_stage逻辑自洽
!

##input
confidence_logic="${label_l2_model_with_confidence_union_logic_di}"
##output
outputTable="${label_l2_model_with_confidence_union_logic_di}"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $outputTable partition(day='$day')
select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,car,car_cl,house,house_cl,married,
       married_cl,occupation,occupation_cl,industry,industry_cl,agebin_1001,agebin_1001_cl,city_level,special_time,consum_level,
       case
         when agebin = 9 and life_stage = '0.0' then '0'
         when agebin = 9 and (life_stage is null or life_stage <> '0.0') then '1'
         when agebin = 8 and occupation = 20 and edu = 6 then '1'
         when (agebin= 7 or agebin = 8) and occupation = 20 then '2'
         when life_stage = '3.0' then '3'
         when kids = 3 then '4'
         when kids = 4 then '5'
         when kids = 5 then '6'
         when kids = 6 then '7'
         else ''
       end as life_stage,
       income_1001,income_1001_cl,occupation_1001,occupation_1001_cl,consume_level,consume_level_cl,agebin_1002,agebin_1002_cl,agebin_1003,agebin_1003_cl
from $confidence_logic
where day='$day'
"
