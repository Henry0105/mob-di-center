#!/bin/bash
set -x -e

: '
@owner:guanyt
@describe: 单纯的把一些statics类的标签合并在一起
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh


##input
label_device_applist_cnt=$label_l1_applist_refine_cnt_di
label_grouplist_di=$label_l1_grouplist_di
label_catelist_di=$label_l1_catelist_di
label_nationality=$device_nationality
##output
device_statics_label=$label_statics_type_all_di

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $device_statics_label partition(day='$day')
select un.device,app.applist,app.tot_install_apps,nn.nationality,nn.nationality_cn,g.tag_list as group_list,cates.catelist,app.processtime as processtime
from
(
  select device
  from
  (
    select device
    from $label_device_applist_cnt
    where day = ${day}

    union all

    select device
    from $label_nationality
    where day=${day}

    union all

    select device
    from $label_grouplist_di
    where day='$day'
  )base
  group by device
)un
left join
(
  select device,cnt as tot_install_apps,applist,'$day' as processtime
  from $label_device_applist_cnt
  where day = ${day}
) app on un.device=app.device
left join
(
  select device, country_code as nationality,nationality as nationality_cn
  from $label_nationality
  where day=${day}
)nn on un.device=nn.device
left join
(
  select device,tag_list
  from $label_grouplist_di
  where day='$day'
) g on un.device=g.device
left join
(
  select *
  from $label_catelist_di
  where day='$day'
) cates on un.device=cates.device;
"
