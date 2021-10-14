#!/bin/bash

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi


day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dm_mobdi_tmp

## 源表
tmp_engine00002_datapre=$tmpdb.tmp_engine00002_datapre

## mapping 表
#dim_vacation_flag_par=dim_sdk_mapping.dim_vacation_flag_par
#vacation_flag=dm_sdk_mapping.vacation_flag

## 目标表
engine00008_data_collect=$tmpdb.engine00008_data_collect

vacation_flag_db=${dim_vacation_flag_par%.*}
vacation_flag_tb=${dim_vacation_flag_par#*.}
sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$vacation_flag_db', '$vacation_flag_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
vacation_flag_lastday=(`hive  -e "$sql"`)

hive -v -e "

insert overwrite table $engine00008_data_collect partition(day='$day')
select device, day as days, city,
case
when flag=1 then '小长假'
when flag=2 then '大长假'
when week_day in (6,7) then '周末'
else '工作日' end as time_frame
from
(
  select t1.device, t1.day, t1.city, t2.flag, pmod(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(t1.day,'yyyyMMdd'))),'1900-01-08'),7)+1 as week_day
  from
(
  select device, day, city
  from
    (select device,days as day,city
    from
    (select device,days,city,city_home,city_work from $tmp_engine00002_datapre where day='$day') tt
    where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0) and length(city)>0
    )a
  group by device, day, city
)t1
left join $dim_vacation_flag_par t2 on t1.day=t2.day and t2.version='$vacation_flag_lastday'
)c
;
"


