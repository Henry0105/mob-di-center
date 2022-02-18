#!/bin/bash
set -x -e

: '
@owner:guanyt
@describe: 单纯的把一些model类的标签合并在一起
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

##input
confidence_logic=$label_l2_model_with_confidence_union_logic_di
label_taglist_di=$label_l1_taglist_di
label_cluster_di=$label_l2_device_cluster_di
label_consume_1001=$label_l1_consume_1001_di
##output
device_model_label=$label_model_type_all_di

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function concat2 as 'com.youzu.mob.java.udaf.concatfortwofields';
insert overwrite table $device_model_label partition(day='$day')
select un.device,user_model.gender,user_model.agebin,user_model.car,user_model.married,user_model.edu,user_model.income,
       user_model.house,user_model.kids,user_model.occupation,user_model.industry,user_model.life_stage,user_model.special_time,
       user_model.consum_level,user_model.agebin_1001,tag.tag_list as tag_list,-1 as repayment,bb.cluster as segment,
       user_model.income_1001_v2 as income_1001,user_model.occupation_1001,user_model.consume_level,consume.consume_1001,
       user_model.gender_cl,user_model.agebin_cl,user_model.car_cl,user_model.married_cl,user_model.edu_cl,
       user_model.income_cl,user_model.house_cl,user_model.kids_cl,user_model.occupation_cl,user_model.industry_cl,
       user_model.agebin_1001_cl,user_model.income_1001_v2_cl as income_1001_cl,user_model.occupation_1001_cl,user_model.consume_level_cl,
       user_model.agebin_1002,user_model.agebin_1002_cl,user_model.agebin_1003,user_model.agebin_1003_cl,
       user_model.occupation_1002,user_model.occupation_1002_cl,gender.gender_1001,gender.gender_1001_cl
from
(
  select device
  from
  (
    select device
    from $confidence_logic
    where day='$day'

    union all

    select device
    from $label_taglist_di
    where day='$day'

    union all

    select device
    from $label_cluster_di
    where day='$day'

    union all

    select device
    from $label_consume_1001 where day='$day'

    union all

    select device
    from $gender_scoring_result_di where day='$day'
  )base
  group by device
) un
left outer join
(
  select *
  from $confidence_logic
  where day='$day'
) user_model on un.device=user_model.device
left outer join
(
  select device, tag_list
  from $label_taglist_di
  where day='$day'
)tag on un.device=tag.device
left outer join
(
  select cluster,device
  from $label_cluster_di
  where day='$day'
) bb on un.device = bb.device
left outer join
(
  select cluster as consume_1001,device
  from $label_consume_1001
  where day='$day'
) consume on consume.device = un.device
left join
(
  select device,gender gender_1001,probability gender_1001_cl
  from $gender_scoring_result_di
  where day='$day'
) gender on un.device=gender.device
;
"
