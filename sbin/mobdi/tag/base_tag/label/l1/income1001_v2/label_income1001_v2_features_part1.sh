#!/bin/sh

set -x -e

: '
@owner:hugl
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=${dw_mobdi_md}

output_table="${tmpdb}.tmp_income1001_part1"
#input
label_merge_all="${tmpdb}.model_merge_all_features"
label_apppkg_category_index=${label_l1_apppkg_category_index}


## part1 完全可以复用年龄标签part1
HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${output_table};
create table if not exists ${output_table} as
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from 
(
  select device
      ,case
        when city_level_1001 = 1 then 0
        when city_level_1001 = 2  then 1
        when city_level_1001 = 3  then 2
        when city_level_1001 = 4  then 3
        when city_level_1001 = 5  then 4
        when city_level_1001 = 6  then 5
        else 6 end as index
       ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device
    ,case
      when factory = 'HUAWEI' then 7
      when factory = 'OPPO' then 8
      when factory = 'VIVO' then 9
      when factory = 'XIAOMI' then 10
      when factory = 'SAMSUNG' then 11
      when factory = 'MEIZU' then 12
      when factory = 'ONEPLUS' then 13
      when factory = 'SMARTISAN' then 14
      when factory = 'GIONEE' then 15
      when factory = 'MEITU' then 16
      when factory = 'LEMOBILE' then 17
      when factory = '360' then 18
      when factory = 'BLACKSHARK' then 19
      else 20 end as index
    ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device
    ,case
      when split(sysver, '\\\\.')[0] = 10 then 21
      when split(sysver, '\\\\.')[0] = 9 then 22
      when split(sysver, '\\\\.')[0] = 8 then 23
      when split(sysver, '\\\\.')[0] = 7 then 24
      when split(sysver, '\\\\.')[0] = 6 then 25
      when split(sysver, '\\\\.')[0] = 5 then 26
      when split(sysver, '\\\\.')[0] <= 4 then 27
      when sysver = 'unknown' then 28
      else 29 end as index
    ,1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
    when diff_month < 12 then 30
    when diff_month >= 12 and diff_month < 24 then 31
    when diff_month >= 24 and diff_month < 36 then 32
    when diff_month >= 36 and diff_month < 48 then 33
    when diff_month >= 48 then 34
  else 35
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
  case
  when tot_install_apps <= 10 then 36
  when tot_install_apps <= 20 then 37
  when tot_install_apps <= 30 then 38
  when tot_install_apps <= 50 then 39
  when tot_install_apps <= 100 then 40
  else 41
  end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,
    case
    when price > 0 and price < 1000 then 42
    when price >= 1000 and price < 1499 then 43
    when price >= 1499 and price < 2399 then 44
    when price >= 2399 and price < 4000 then 45
    when price >= 4000 then 46
    else 47
    end as index, 1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all
  
  select device,
    case
    when house_price >= 0 and house_price < 8000 then 48
    when house_price >= 8000 and house_price < 12000 then 49
    when house_price >= 12000 and house_price < 22000 then 50
    when house_price >= 22000 and house_price < 40000 then 51
    when house_price >= 40000 and house_price < 60000 then 52
    when house_price >= 60000 then 53
    else 54
    end as index,
    1.0 as cnt
  from $label_merge_all
  where day = ${day}

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.age.cate_l1'

  union all

  select device,index,cnt from $label_apppkg_category_index where day = '${day}' and version = '1003.age.cate_l2'
)a
group by device
;
"
