#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 利用每日的增量，获取到新的标签，最新的life_stage需要的
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dim_device_applist_new_di=dim_mobdi_mapping.dim_device_applist_new_di
#mapping
#dim_apppkg_name_info_wf=dim_mobdi_mapping.dim_apppkg_name_info_wf
#mapping_life_stage_applist=tp_sdk_model.mapping_life_stage_applist

#output
outputTable="${label_l2_result_scoring_di}"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $outputTable partition (day=$day, kind='life_stage')
select device,life_stage as prediction, 1.0 as probability
from
(
  select a2.device,a1.life_stage,
         row_number() over (partition by device order by life_stage desc) as rank
  from
  (
    select life_stage,apppkg
    from
    (
      select 0 as life_stage,apppkg
      from $dim_apppkg_name_info_wf
      where app_name like '%小学%'
      and day = ${day}
      union all

      select 3 as life_stage,apppkg
      from $mapping_life_stage_applist
      where life_stage in ('备孕','孕期')
    )a
  )a1
  inner join
  (
    select device, pkg as apppkg
    from $dim_device_applist_new_di
    where day = ${day}
  ) a2 on a1.apppkg = a2.apppkg
)aa
where rank = 1
"
