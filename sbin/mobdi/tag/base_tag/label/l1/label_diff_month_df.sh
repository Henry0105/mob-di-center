#!/bin/bash

# 有疑问,待完成
# 作为pre_data的输入
#source /home/mobdi_test/public/util/util.sh


source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1
year=`date -d "${day}" +%Y`
month=`date -d "${day}" +%m`

## input table
#device_info_master_full_par="dm_mobdi_master.device_info_master_full_par"
device_info_master_full_par="dm_mobdi_master.dwd_device_info_df"

## taget table
label_diff_month_df=${label_l1_diff_month_df}

hive -v -e  "
INSERT overwrite table $label_diff_month_df partition(day='$day')
select device,
       case
         when length(trim(public_date)) = 4 and trim(public_date) not in ('NULL','null') then if(public_date = ${year}, 6, ((${year} - public_date)*12))
         when length(trim(public_date)) = 5 then if(split(public_date, '年')[0] = ${year}, 6, (${year} - split(public_date, '年')[0])*12)
         when length(trim(public_date)) >= 7 and trim(public_date) not rlike '^[0-9]{8,9}$'
           then (${year} - split(public_date, '年')[0])*12 + ${month} - split(split(public_date, '年')[1], '月')[0]
         when trim(public_date) rlike '^[0-9]{8,9}$'
           then if(substring(public_date, 1, 4) = ${year}, 6, (${year} - substring(public_date, 1, 4))*12 + ${month} - substring(public_date, 5, 2))
         else NULL
       end as diff_month
from $device_info_master_full_par
where version = '$day.1000'
and plat = '1'
"
