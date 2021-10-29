#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 利用每日的增量，获取到新的标签，最新的city_level,city_level_1001
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1
pday=`date -d "$day -1 days" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"

#input
label_hometown_di=$label_l1_citylevel_di
#mapping
#output
label_hometown_df=$label_l2_citylevel_df

: "
刷历史全量数据
insert overwrite table rp_mobdi_app.label_l2_citylevel_df partition(day='20200304')
select device,
       if(country='unknown','',country) as country,
       if(province='unknown','',province) as province,
       if(city='unknown','',city) as city,
       if(country_cn='未知','',country_cn) as country_cn,
       if(province_cn='未知','',province_cn) as province_cn,
       if(city_cn='未知','',city_cn) as city_cn,
       city_level,
       city_level_1001,
       last_active as process_time
from rp_mobdi_app.device_profile_label_full_par
where version='20200304.1000';
"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $label_hometown_df partition (day=$day)
select device,country,province,city,country_cn,province_cn,city_cn,city_level,city_level_1001,process_time
from
(
  select device,country,province,city,country_cn,province_cn,city_cn,city_level,city_level_1001,process_time,
         row_number() over(partition by device order by process_time desc) as rank
  from
  (
    select device,country,province,city,country_cn,province_cn,city_cn,city_level,city_level_1001,process_time
    from $label_hometown_df
    where day=$pday
    union all
    select device,country,province,city,country_cn,province_cn,city_cn,city_level,city_level_1001,day as process_time
    from $label_hometown_di
    where day=$day
  )t
)tt
where rank=1
"