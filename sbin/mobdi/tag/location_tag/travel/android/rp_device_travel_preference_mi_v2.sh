#!/bin/bash

set -x -v

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dm_mobdi_tmp
## 源表
engine00002_data_collect=$tmpdb.engine00002_data_collect
engine00003_data_collect=$tmpdb.engine00003_data_collect
engine00004_data_collect=$tmpdb.engine00004_data_collect
engine00005_data_collect=$tmpdb.engine00005_data_collect
engine00006_data_collect=$tmpdb.engine00006_data_collect
engine00007_data_collect=$tmpdb.engine00007_data_collect
engine00008_data_collect=$tmpdb.engine00008_data_collect
engine00010_data_collect=$tmpdb.engine00010_data_collect
engine00011_data_collect=$tmpdb.engine00011_data_collect


## 目标表
#rp_device_travel_preference_mi_v2=dm_mobdi_report.rp_device_travel_preference_mi_v2


hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function complex_to_null as 'com.youzu.mob.java.udf.ComplexTypeToNull';

SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.support.concurrency=false;

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;

insert overwrite table $rp_device_travel_preference_mi_v2 partition(day='$day')
select coalesce(ttt1.device,ttt2.device,ttt3.device,ttt4.device) as device,days,
       if(business_score='',null,business_score) as business_score,
       business_avg_score,
       if(normal_score='',null,normal_score) as normal_score,
       normal_avg_score,
       if(level_citylist='',null,level_citylist) as level_citylist,
       if(levellist='',null,levellist) as levellist,
       if(length(frame_citylist)=0,null,frame_citylist) as frame_citylist,
       if(length(frame)=0,null,frame) as frame,
       travel_type,traffic_day,traffic_list,
       continents,country_code,country_poi,province_code,province_poi,city_code,city_poi,
       citylist,
       if(length(time)=0,null,time) as time,
       if(time is null or length(trim(time))<=0,null,size(split(time,','))) as time_cnt,round(avg_time,2) as avg_time,
       complex_to_null(hotel_style) as hotel_style,
complex_to_null(rank_star) as rank_star,
complex_to_null(score_type) as score_type,
complex_to_null(price_level) as price_level,
complex_to_null(brand) as brand
from
(select device,
       concat_ws(',',collect_list(distinct(days))) as days,
       if(size(collect_list(distinct(days)))=size(collect_list(cast(business_score as string))),concat_ws(',',collect_list(cast(business_score as string))),null) as business_score,
       if(size(collect_list(distinct(days)))=size(collect_list(cast(business_score as string))), max(business_avg_score),null) as business_avg_score,
       if(size(collect_list(distinct(days)))=size(collect_list(cast(business_score as string))),concat_ws(',',collect_list(cast(normal_score as string))),null) as normal_score,
       if(size(collect_list(distinct(days)))=size(collect_list(cast(business_score as string))),max(normal_avg_score),null) as normal_avg_score,
       concat_ws('|',collect_list(level_citylist)) as level_citylist,
       concat_ws('|',collect_list(levellist)) as levellist,
       concat_ws(',',collect_list(frame_city)) as frame_citylist,
       concat_ws(',',collect_list(frame)) as frame,
       concat_ws(',',collect_list(cast(travel_type as string))) as travel_type
from(
select
device,days,
if(max(business_score)='-0','0.0',max(business_score)) as business_score,
if(max(business_avg_score)=-2,0.0,max(business_avg_score)) as business_avg_score,
if(max(normal_score)='-0','0.0',max(normal_score)) as normal_score,
if(max(normal_avg_score)=-2,0.0,max(normal_avg_score)) as normal_avg_score,
max(level_citylist) as level_citylist,
max(levellist) as levellist,
concat_ws(',',collect_list(frame_city)) as frame_city,
concat_ws(',',collect_list(frame)) as frame,
max(travel_type) as travel_type
from(
select device,days,
       if(business_score is null and normal_score is not null,'-0',business_score) as business_score,
       if(business_score is null and normal_score is not null,-2,business_avg_score) as business_avg_score,
       if(business_score is not null and normal_score is null,'-0',normal_score) as normal_score,
       if(business_score is not null and normal_score is null,-2,normal_avg_score) as normal_avg_score,
       level_citylist,levellist,frame_city,frame,travel_type
from
(
select device,days,score as business_score ,avg_score as business_avg_score,
           null as normal_score,null as normal_avg_score
           ,null as level_citylist ,null as levellist,
           null as frame_city,null as frame,
           null as travel_type
from
$engine00002_data_collect
where day='$day' and (score is not null or avg_score is not null)
group by device,days,score,avg_score

union all

select device,days,null as business_score ,null as business_avg_score,
           score as normal_score ,avg_score as normal_avg_score,
           null as level_citylist ,null as levellist,
           null as frame_city,null as frame,
           null as travel_type
from
$engine00003_data_collect
where day='$day' and (score is not null or avg_score is not null) and days<>'20200101'
group by device,days,score,avg_score

union all

select device, days,null as business_score ,null as business_avg_score,
           null as normal_score ,null as normal_avg_score,
           citylist as level_citylist ,levellist as levellist,
           null as frame_city,null as frame,
           null as travel_type
from
$engine00006_data_collect
where day='$day' and (citylist is not null or levellist is not null)
group by device,days,citylist,levellist

union all

select device,days,null as business_score ,null as business_avg_score,
           null as normal_score ,null as normal_avg_score,
           null as level_citylist ,null as levellist,
           concat_ws(',',collect_list(city)) as frame_city,concat_ws(',',collect_list(time_frame)) as frame,
           null as travel_type
from(
select device,days,time_frame,city
from
$engine00008_data_collect
where day='$day' and  (time_frame is not null or city is not null)
group by device,days,time_frame,city
)aa
group by device,days


union all

select device,days,null as business_score ,null as business_avg_score,
           null as normal_score ,null as normal_avg_score,
           null as level_citylist ,null as levellist,
           null as frame_city,null as frame,
           travel_type as travel_type
from
$engine00010_data_collect
where day='$day' and travel_type is not null
group by device,days,travel_type
)aa
)tt1
group by device,days
cluster by device,days
)tt2
group by device
) ttt1

full join

(select device,
concat_ws(',',collect_list(days)) as traffic_day,
concat_ws('|',collect_list(concat_ws(',',traffic_list))) as traffic_list
from
(select * from $engine00005_data_collect where day='$day') aa
group by device) ttt2
on ttt1.device=ttt2.device

full join
(
select
device,
concat_ws(',',collect_list(continents)) as continents,
concat_ws(',',collect_list(country_code)) as country_code,
concat_ws(',',collect_list(country_poi)) as country_poi,
concat_ws(',',collect_list(province_code)) as province_code,
concat_ws(',',collect_list(province_poi)) as province_poi,
concat_ws(',',collect_list(city_code)) as city_code,
concat_ws(',',collect_list(city_poi)) as city_poi

from(
select
device,continents,country_code,country_poi,province_code,province_poi,city_code,city_poi
 from
(select * from $engine00007_data_collect where day='$day' and continents not like 'cn%') aa
group by
device,continents,country_code,country_poi,province_code,province_poi,city_code,city_poi
) aa
group by device
) ttt3
on ttt1.device=ttt3.device

full join

(
select device,
       concat_ws(',',collect_list(city)) as citylist,
       concat_ws(',',collect_list(time)) as time,
       max(avg_time) as avg_time,
       collect_list(hotel_style) as hotel_style,
       collect_list(rank_star) as rank_star,
       collect_list(score_type) as score_type,
       collect_list(price_level) as price_level,
       collect_list(brand) as brand
from(
select
coalesce(t1.device,t2.device) as device,coalesce(t1.city,t2.city) as city,time,avg_time,hotel_style,rank_star,score_type,price_level,brand
from
(select device,city as city,time as time,avg_score as avg_time
from $engine00004_data_collect
where day='$day' and (time is not null and avg_score is not null and length(time)>0)
group by device,city,time,avg_score)t1
left join
(select device,city as city,
        hotel_style as hotel_style,rank_star as rank_star,
        score_type as score_type,price_level as price_level,
        brand as brand
from $engine00011_data_collect
where day='$day' and (hotel_style is not null or rank_star is not null
      or score_type is not null or price_level is not null or brand is not null)
group by device,city,hotel_style,rank_star,score_type,price_level,brand) t2
on t1.device=t2.device and t1.city=t2.city
)t1
group by device
)ttt4
on ttt1.device=ttt4.device
where coalesce(ttt1.device,ttt2.device,ttt3.device,ttt4.device) is not null
;
"
