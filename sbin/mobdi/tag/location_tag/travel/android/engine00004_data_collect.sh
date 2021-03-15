#!/bin/bash



if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

source ../../../../util/util.sh

day=$1

location_day=${day:0:6}01
p1=`date -d "${location_day} -1 days" +%Y%m%d`


## 源表
tmp_engine00002_datapre=dm_mobdi_tmp.tmp_engine00002_datapre

## 目标表
engine00004_data_collect=dm_mobdi_tmp.engine00004_data_collect

sql_final="
create temporary function city_day_count as 'com.youzu.mob.java.udf.TravelCityDayCount';
create temporary function get_need_city as 'com.youzu.mob.java.udf.GetNeedCity';

insert overwrite table $engine00004_data_collect partition (day='$location_day')
select device,split(city_time,':')[0] as city,split(city_time,':')[1] as time,
avg(cast(split(city_time,':')[1] as double)) over (partition by device) as avg_score
from(
select device,city_time
from(
select device,city_day_count(city_day_list,'$p1') as city_day_list
from(
select
device,concat_ws(',',sort_array(collect_list(concat(starttime,':',city)))) as city_day_list
from
(
select tt2.*
from
(
select device,day,city
from(
select device,days as day,get_need_city(concat_ws(',',sort_array(collect_list(concat(starttime,':',city))))) as city_day_list
from
$tmp_engine00002_datapre
where day='$location_day' and city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
and city is not null and length(trim(city))>0 and length(starttime)=10
group by
device,days
)t1
LATERAL VIEW explode(split(city_day_list,',')) t  as city
) tt1

inner join
(select *
from
$tmp_engine00002_datapre
where day='$location_day' and city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
and city is not null and length(trim(city))>0 and length(starttime)=10
)tt2
on tt1.device=tt2.device and tt1.day=tt2.days and tt1.city=tt2.city
) tt
where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
and city is not null and length(trim(city))>0 and length(starttime)=10
group by device
)aa
)cc
LATERAL VIEW explode(split(city_day_list,',')) t as city_time 
)tt;
"
hive_setting "$sql_final"