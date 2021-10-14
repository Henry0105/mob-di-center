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

p30=`date -d "${day_run} -1 months" +%Y%m%d`
#dim_apppkg_name_info_wf=dim_mobdi_mapping.dim_apppkg_name_info_wf

value=`hive -e "show partitions $dim_apppkg_name_info_wf"|tail -n 1`

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb=$dm_mobdi_tmp
## 源表
#dws_device_install_app_status_40d_di=dm_mobdi_topic.dws_device_install_app_status_40d_di
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
tmp_engine00002_datapre=$tmpdb.tmp_engine00002_datapre
#device_profile_label_full_par=dm_mobdi_report.device_profile_label_full_par

## mapping表
#dim_apppkg_name_info_wf=dim_mobdi_mapping.dim_apppkg_name_info_wf
#apppkg_name_info_wf=dm_mobdi_mapping.apppkg_name_info_wf
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par
#dim_app_business_trip_list=dim_sdk_mapping.dim_app_business_trip_list
#app_business_trip_list=dm_sdk_mapping.app_business_trip_list
#dim_vacation_flag_par=dim_sdk_mapping.dim_vacation_flag_par
#vacation_flag=dm_sdk_mapping.vacation_flag

## 目标表
engine00002_data_collect=$tmpdb.engine00002_data_collect

app_pkg_mapping_par_db=${dim_app_pkg_mapping_par%.*}
app_pkg_mapping_par_tb=${dim_app_pkg_mapping_par#*.}
profile_label_full_par_db=${device_profile_label_full_par%.*}
profile_label_full_par_tb=${device_profile_label_full_par#*.}
vacation_flag_db=${dim_vacation_flag_par%.*}
vacation_flag_tb=${dim_vacation_flag_par#*.}


## 获取最新分区
sql1="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$app_pkg_mapping_par_db', '$app_pkg_mapping_par_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
app_pkg_mapping_par_lastday=(`hive  -e "$sql1"`)

sql2="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$vacation_flag_db', '$vacation_flag_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
vacation_flag_lastday=(`hive  -e "$sql2"`)


sql3="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$profile_label_full_par_db', '$profile_label_full_par_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
device_profile_label_full_par_lastday=(`hive  -e "$sql3"`)


sql_final="
SET mapreduce.map.memory.mb=8192;
SET mapreduce.map.java.opts='-Xmx6g';
SET mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8196;
SET mapreduce.reduce.java.opts='-Xmx6g';
SET mapreduce.map.java.opts='-Xmx6g';


insert overwrite table $engine00002_data_collect partition(day='$location_day')
select nvl(dd1.device,dd2.device) as device,nvl(dd1.day,dd2.day) as day,
       case when dd2.device is not null then -1
            else round(score,2) end  as score,
       case when dd2.device is not null and (round((avg(score) over(partition by dd1.device)),2)<=0 or round((avg(score) over(partition by dd1.device)),2) is null) then -1
            else round((avg(score) over(partition by dd1.device)),2) end as avg_score
from(
select device,day,sum(score) as score
from(
select device,day,
     case  when flag = 1 then score*0.4
          when flag = 2 then score*0.2
     else score end as score
from
(select device, flag,day,
        case when  active_flag = '1' then sum(install_score)
             else sum(active_score) end as score
 from (
        select device, ttt1.day, active_flag, flag,install_score, active_score
        from (
            select tt1.device, tt1.day, tt1.active_flag,if(tt2.device is not null,tt1.install_score,if(tt1.install_score is not null,0,null)) as install_score,if(tt2.device is not null,tt1.active_score,if(tt1.active_score is not null,0,null)) as active_score
            from (
                select t1.device, t1.pkg, t1.active_flag,t1.day, t2.apppkg,if(t2.install_score is not null,t2.install_score,if(t1.pkg is not null,0,null)) as install_score,if(t2.active_score is not null,t2.active_score,if(t1.pkg is not null,0,null)) as active_score
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
                    where day <='$day_run' and day > '$p30'
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
                        from (select * from $dim_apppkg_name_info_wf where $value) a1
                        inner join (select * from  $dim_app_business_trip_list where version='1000') a2 on a1.app_name = a2.appname
                        ) b1
                    left join (
                        select pkg, apppkg
                        from $dim_app_pkg_mapping_par
                        where version = '$app_pkg_mapping_par_lastday'
                        ) b2 on b1.apppkg = b2.apppkg
                    ) t2 on t1.pkg = t2.pkg
                ) tt1
                 left join (
                select c1.device
                from (
                    select device from (select * from $tmp_engine00002_datapre where day='$location_day') aa
                    where city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0 )
                      group by device
                    ) c1
                 join (
                    select device, agebin, occupation
                    from $device_profile_label_full_par
                    where version='$device_profile_label_full_par_lastday'
                    ) c2 on c1.device = c2.device
                where c2.agebin <> 9 and c2.occupation <> 20
                group by c1.device
                ) tt2 on tt1.device = tt2.device
            ) ttt1
        left join $dim_vacation_flag_par ttt2 on ttt1.day = ttt2.day and ttt2.version= '$vacation_flag_lastday'
        ) a
        group by device,day,flag, active_flag) b
group by device,day,flag,score
)cc
group by device,day
)dd1

full join

(select device,days as day
              from $tmp_engine00002_datapre
              where day='$location_day' and city <> city_home and city <> city_work and (length(city_home) > 0 or length(city_work) > 0) and  (length(city)>0 and country<>'cn')
group by device,days) dd2
on dd1.device=dd2.device and dd1.day=dd2.day
"

hive_setting "$sql_final"

