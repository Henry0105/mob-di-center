#!/bin/bash

set -e -x

: '
   生成置信度全量表
   处理流程：
   1.dw_mobdi_md.models_with_confidence_union_logic_par与dw_mobdi_md.device_cate_preference_incr关联得到设备app分类偏好度列表增量数据,
     结果再与rp_mobdi_app.device_models_confidence_full的最新分区数据合并去重, 保留每个设备的最新数据, 存入
     rp_mobdi_app.device_models_confidence_full表的最新分区
   2.视图rp_mobdi_app.device_models_confidence_full_view指向rp_mobdi_app.device_models_confidence_full的最新分区数据
   3.删除rp_mobdi_app.device_models_confidence_full过期分区, 只保留最近五个分区
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb="dw_mobdi_tmp"

##input
device_cate_preference_incr=$tmpdb.device_cate_preference_incr
label_l2_model_with_confidence_union_logic_di=$label_l2_model_with_confidence_union_logic_di

##output
device_models_confidence_full=$device_models_confidence_full

new_ver=${day}.1000

lastPartStr=`hive -e "show partitions $device_models_confidence_full" | sort | tail -n 1`

if [ -z "$lastPartStr" ]; then
    lastPartStrA=$lastPartStr
fi

if [ -n "$lastPartStr" ]; then
    lastPartStrA=" AND  $lastPartStr"
fi

echo $lastPartStrA

hive -v -e"
insert overwrite table $device_models_confidence_full partition(version='$new_ver')
select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
       car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
       industry_cl,agebin_1001,agebin_1001_cl,processtime,coalesce(cate_preference_list,''),income_1001,income_1001_cl,
       occupation_1001,occupation_1001_cl,consume_level,consume_level_cl
from
(
  select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
         car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
         industry_cl,agebin_1001,agebin_1001_cl,processtime,cate_preference_list,income_1001,income_1001_cl,
         occupation_1001,occupation_1001_cl,consume_level,consume_level_cl,
         row_number() over(partition by device order by processtime desc) rn
  from
  (
    select confidence.device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
           car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
           industry_cl,agebin_1001,agebin_1001_cl,day as processtime,coalesce(cate_preference_list,'') as cate_preference_list,
           income_1001,income_1001_cl,occupation_1001,occupation_1001_cl,consume_level,consume_level_cl
    from $label_l2_model_with_confidence_union_logic_di confidence
    left join
    (
      select device,
             concat(concat_ws(',',collect_list(cate_id)),'=',concat_ws(',',collect_list(cast(preference as string)))) as cate_preference_list
      from $device_cate_preference_incr
      where day = '${day}'
      group by device
    ) preference on confidence.device=preference.device
    where confidence.day='${day}'

    union all

    select device,gender,gender_cl,agebin,agebin_cl,edu,edu_cl,income,income_cl,kids,kids_cl,
           car,car_cl,house,house_cl,married,married_cl,occupation,occupation_cl,industry,
           industry_cl,agebin_1001,agebin_1001_cl,processtime,cate_preference_list,income_1001,income_1001_cl,
           occupation_1001,occupation_1001_cl,consume_level,consume_level_cl
    from $device_models_confidence_full
    where 1=1 ${lastPartStrA}
  ) un 
) tmp 
where rn=1;
"

#实现删除过期的分区的功能，只保留最近5个分区
for old_version in `hive -e "show partitions $device_models_confidence_full " | grep -v '_bak' | sort | head -n -10`
do 
    echo "rm $old_version"
    hive -v -e "alter table $device_models_confidence_full drop if exists partition($old_version)"
done

#qc
lastDay=`date -d "$day -1 days" +%Y%m%d`
qc_success_flag=0
#如果qc_before_view.sh脚本执行失败，说明qc失败，将qc_success_flag置为1
cd `dirname $0`
/home/dba/mobdi/qc/real_time_mobdi_qc/qc_before_view.sh "${new_ver}" "${lastDay}.1000" "$device_models_confidence_full" || qc_success_flag=1
if [[ ${qc_success_flag} -eq 1 ]]; then
  echo 'qc失败，阻止生成view'
  exit 1
fi

hive -v -e "
create or replace view rp_mobdi_report.device_models_confidence_full_view as
select *
from $device_models_confidence_full
where version='${new_ver}';
"

echo "run over of $day"
