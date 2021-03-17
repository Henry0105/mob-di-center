#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 利用每日的增量，获取到新的标签，增量的kids score
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

##input
device_applist_new=${dim_device_applist_new_di}
#mapping
kids_pkg03="tp_mobdi_model.kids_pkg03"
kids_pkg37="tp_mobdi_model.kids_pkg37"
kids_pkg712="tp_mobdi_model.kids_pkg712"
#output
label_kids_score_di=${label_l1_kids_score_di}

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_kids_score_di partition(day='$day')
select device, max(tag) as label
from
(
  select pkg, 1.0 as tag
  from $kids_pkg03

  union all

  select pkg, 2.0 as tag
  from $kids_pkg37

  union all

  select pkg, 3.0 as tag
  from $kids_pkg712
) as a1
inner join
(
  select device, pkg
  from $device_applist_new
  where day = '$day'
) as a2 on a1.pkg = a2.pkg
group by device;
"
