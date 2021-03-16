#!/bin/bash

source ../../../../util/util.sh

## 文旅标签 出行人群

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

location_day=${day:0:6}01

day_run=`date +%Y%m%d -d "${location_day} -1 month"`

p30=`date +%Y%m%d -d "${day_run} -1 month"`
value=`hive -e "show partitions dm_mobdi_mapping.apppkg_name_info_wf"|tail -n 1`

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

## 源表
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
tmp_engine00002_datapre=dm_mobdi_tmp.tmp_engine00002_datapre

## mapping表
#apppkg_name_info_wf=dm_mobdi_mapping.apppkg_name_info_wf
#app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par
#app_normal_trip_list=dm_sdk_mapping.app_normal_trip_list
#vacation_flag=dm_sdk_mapping.vacation_flag
#travel_poi_with_boundary=dm_mobdi_mapping.travel_poi_with_boundary

## 目标表
engine00003_data_collect=dm_mobdi_tmp.engine00003_data_collect


## 获取最新分区
sql1="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_sdk_mapping', 'app_pkg_mapping_par', 'version');
drop temporary function GET_LAST_PARTITION;
"
app_pkg_mapping_par_lastday=(`hive  -e "$sql1"`)

## 获取最新分区
sql2="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'travel_poi_with_boundary', 'version');
drop temporary function GET_LAST_PARTITION;
"
travel_poi_with_boundary_lastday=(`hive  -e "$sql2"`)


sql_final="
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';
set hive.exec.mode.local.auto=false;
set hive.auto.convert.join=false;

create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

insert overwrite table $engine00003_data_collect partition(day='$location_day')
select nvl(dd1.device,dd2.device) as device,nvl(dd1.day,dd2.day) as day,
       case when dd2.device is not null then -1
            else round(score,2) end  as score,
       case when dd2.device is not null and (round((avg(score) over(partition by dd1.device)),2)<=0 or round((avg(score) over(partition by dd1.device)),2) is null) then -1
            else round((avg(score) over(partition by dd1.device)),2) end as avg_score

from(
select device,day,sum(score) as score
from(
select device,day,case when flag = 1 then score*0.4
     when flag = 2 then score*0.2
     else score end as score
from
(select device, flag,day,
        case when active_flag = '1' then sum(install_score) else sum(active_score) end as score from (
        select device, ttt1.day, active_flag, flag, install_score, active_score
        from (
            select tt1.device, tt1.day, tt1.active_flag,tt1.install_score,tt1.active_score
            from (
                select t1.device, t1.pkg, t1.active_flag,t1.day, t2.apppkg, if(t2.install_score is not null,t2.install_score,if(t1.pkg is not null,0,null)) as install_score,if(t2.active_score is not null,t2.active_score,if(t1.pkg is not null,0,null)) as active_score

                from (
                    select d1.device, d1.pkg,d1.active_flag,d2.day
                    from(
                    select device, pkg, '1' as active_flag
                    from $dws_device_install_app_status_40d_di
                    where day = '$day_run'
                    group by device,pkg

                    union all

                    select device, pkg, '2' as active_flag
                    from $dws_device_active_applist_di
                    where day <='$day_run' and day >'$p30'
                    group by device,pkg
                    )d1
                    right join
                    (
                    select device,days as day
                    from (select * from $tmp_engine00002_datapre where day='$location_day') tt
                    where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0)
                    and length(city)>0 and country='cn'
                    group by device,days
                    )d2
                    on d1.device=d2.device
                    ) t1
                left join (
                    select coalesce(b2.pkg,b1.apppkg) as pkg, b1.apppkg, b1.install_score, b1.active_score
                    from (
                        select a1.apppkg, a2.install_score, a2.active_score
                        from (select * from $apppkg_name_info_wf where $value) a1
                        inner join (select * from $app_normal_trip_list where version='1000') a2 on a1.app_name = a2.appname
                        ) b1
                    left join (
                        select pkg, apppkg
                        from $app_pkg_mapping_par
                        where version = '$app_pkg_mapping_par_lastday'
                        ) b2 on b1.apppkg = b2.apppkg
                    ) t2 on t1.pkg = t2.pkg
                ) tt1
            ) ttt1
        left join $vacation_flag ttt2 on ttt1.day = ttt2.day
        ) a
        group by device,day,flag, active_flag) b
group by device,day,flag,score
)cc
group by device,day


union all

select
  device , day , 2*count(1) score
from
  (
    select
      device , day , name
    from
      (
        select
          device , day , name , row_number() over(partition by device,day,starttime order by
                                                  name) rn
        from
          (
            select
              t1.device , days as day , t1.starttime , t2.name
            from
              (
                select device,days,lat,lon,starttime
                from
                  $tmp_engine00002_datapre
                where
                  day='$location_day'
                  and city     <> city_home
                  and city <> city_work
                  and
                  (
                    length(city_home)    > 0
                    or length(city_work) > 0
                  )
              )
              t1
              inner join
                (select * from $travel_poi_with_boundary where  version='$travel_poi_with_boundary_lastday') t2
                on
                  get_geohash(t1.lat,t1.lon,6) = t2.inside_geo6
            where
              is_in( concat(t1.lon,',',t1.lat), t2.baidu_lat_lon_boundary)=1
          )
          tt1
      )
      tt2
    where
      rn=1
    group by
      device , day , name
  )
  tt3
group by
  device , day
)dd1
full join

(select device,days as day
              from $tmp_engine00002_datapre
              where day='$location_day' and city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0) and country <>'cn' and length(city)>0 group by device,days) dd2
on dd1.device=dd2.device and dd1.day=dd2.day
"

hive_setting "$sql_final"