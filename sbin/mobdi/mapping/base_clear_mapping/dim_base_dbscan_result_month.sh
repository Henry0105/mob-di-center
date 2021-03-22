#!/bin/bash

set -x -e

daypre=$1
day=${daypre:0:6}01

p1months=`date -d "$day -1 month" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
#dwd_base_station_info_sec_di=dm_mobdi_master.dwd_base_station_info_sec_di


#mid
mid_dbscan_data_pre_month_part1=dw_mobdi_tmp.mid_dbscan_data_pre_month_part1
mid_dbscan_data_pre_month_part2=dw_mobdi_tmp.mid_dbscan_data_pre_month_part2
mid_dbscan_data_process_month_step3=dw_mobdi_tmp.mid_dbscan_data_process_month_step3
mid_dbscan_data_pre_month=dw_mobdi_tmp.mid_dbscan_data_pre_month
mid_dbscan_data_process_month_step1=dw_mobdi_tmp.mid_dbscan_data_process_month_step1
mid_dbscan_data_process_month_step2=dw_mobdi_tmp.mid_dbscan_data_process_month_step2
mid_dbscan_data_process_month_step3=dw_mobdi_tmp.mid_dbscan_data_process_month_step3
mid_dbscan_data_process_month_step4=dw_mobdi_tmp.mid_dbscan_data_process_month_step4
mid_dbscan_data_final_month_step1=dw_mobdi_tmp.mid_dbscan_data_final_month_step1
mid_dbscan_data_final_month_step2=dw_mobdi_tmp.mid_dbscan_data_final_month_step2
mid_dbscan_data_final_month_step3=dw_mobdi_tmp.mid_dbscan_data_final_month_step3
mid_dbscan_data_result_month=dw_mobdi_tmp.mid_dbscan_data_result_month

# mapping
#geohash6_area_mapping_par=dm_sdk_mapping.geohash6_area_mapping_par
#geohash8_lbs_info_mapping_par=dm_sdk_mapping.geohash8_lbs_info_mapping_par
#mapping_base_station_location=dm_sdk_mapping.mapping_base_station_location
#output
#dim_base_dbscan_result_month=dm_mobdi_mapping.dim_base_dbscan_result_month


## 最新分区
dim_base_dbscan_result_month_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_base_dbscan_result_month', 'day');
"
dim_base_dbscan_result_month_partition=(`hive -e "$dim_base_dbscan_result_month_sql"`)


HADOOP_USER_NAME=dba hive -v -e"
drop table if exists $mid_dbscan_data_pre_month_part1;
create table $mid_dbscan_data_pre_month_part1 stored as orc as
select muid as device,datetime,cl['ltime'] ltime,cl['accuracy'] accuracy,
case when carrier in ('46000', '46002', '46004', '46007', '46008') then 0
    when carrier in ('46001', '46006', '46009', '46010') then 1
    else '' end as mnc,
cast(lac as int) lac,
cast(cell as int) cell,
split(coordinate_converter(cl['longitude'],cl['latitude'],'wgs84','bd09'),',')[1] latitude,
split(coordinate_converter(cl['longitude'],cl['latitude'],'wgs84','bd09'),',')[0] longitude
from $dwd_base_station_info_sec_di
where day<'$day'
and day>='$p1months'
and cl['latitude']!=''
and cl['longitude']!=''
and (bid is null and sid is null and nid is null)
and (lac is not null and cell is not null)
and carrier in ('46000', '46002', '46004', '46007', '46008','46001', '46006', '46009', '46010');


drop table if exists $mid_dbscan_data_pre_month_part2;
create table $mid_dbscan_data_pre_month_part2 stored as orc as
select muid as device,datetime,cl['ltime'] ltime,cl['accuracy'] accuracy,
sid as mnc,nid as lac,bid as cell,
split(coordinate_converter(cl['longitude'],cl['latitude'],'wgs84','bd09'),',')[1] latitude,
split(coordinate_converter(cl['longitude'],cl['latitude'],'wgs84','bd09'),',')[0] longitude
from $dwd_base_station_info_sec_di
where day<'$day'
and day>='$p1months'
and cl['latitude']!=''
and cl['longitude']!=''
and (bid is not null and sid is not null and nid is not null)
and (lac is null and cell is null)
and carrier in ('46003', '46005', '46011', '46012')
and ((sid>=10000 and sid<=20000) or (sid=cast(substr(carrier,4,2) as int)));



drop table if exists $mid_dbscan_data_pre_month;
create table $mid_dbscan_data_pre_month stored as orc as
select lac,cell,mnc,device,
from_unixtime(floor(ltime/1000),'yyyyMMdd') as date,
from_unixtime(floor(ltime/1000),'HH') as hour,
round(latitude,5) lat,round(longitude,5) lon from
(select * from $mid_dbscan_data_pre_month_part1
union all
select * from $mid_dbscan_data_pre_month_part2)a
where accuracy>0
and (datetime-ltime)/1000<=120;



drop table if exists $mid_dbscan_data_process_month_step1;
create table $mid_dbscan_data_process_month_step1 stored as orc as
select lac,cell,mnc,device,date,hour,lat,lon
from $mid_dbscan_data_pre_month
group by lac,cell,mnc,device,date,hour,lat,lon;


drop table if exists $mid_dbscan_data_process_month_step2;
create table $mid_dbscan_data_process_month_step2 stored as orc as
select a.*,b.flag_outlier
from $mid_dbscan_data_process_month_step1 a
left outer join
(select lac,cell,mnc,case when count(1)<=4000 then 0 else 1 end as flag_outlier
from $mid_dbscan_data_process_month_step1
group by lac,cell,mnc)b
on a.lac=b.lac
and a.cell=b.cell
and a.mnc=b.mnc;



drop table if exists $mid_dbscan_data_process_month_step3;
create table $mid_dbscan_data_process_month_step3 stored as orc as
select * from
  (select *,row_number() over(partition by lac,cell,flag_outlier order by rand()) as rn
from $mid_dbscan_data_process_month_step2
where flag_outlier=1)a
where rn<=4000;


drop table if exists $mid_dbscan_data_process_month_step4;
create table $mid_dbscan_data_process_month_step4 stored as orc as
select lac,cell,mnc,device,date,hour,lat,lon
from $mid_dbscan_data_process_month_step2
where flag_outlier=0
union all
select lac,cell,mnc,device,date,hour,lat,lon
from $mid_dbscan_data_process_month_step3;

"

spark2-submit --master yarn \
--executor-memory 8G \
--driver-memory 4G \
--executor-cores 2 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.default.parallelism=6000 \
--conf spark.sql.shuffle.partitions=6000 \
--name "app_status" \
--deploy-mode cluster \
--class com.youzu.mob.base.BaseCluster \
--conf spark.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=5g \
--conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+UseG1GC" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar "$mid_dbscan_data_process_month_step4" "$mid_dbscan_data_result_month"





HADOOP_USER_NAME=dba hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

drop table if exists $mid_dbscan_data_final_month_step1;
create table $mid_dbscan_data_final_month_step1 stored as orc as
select lac,cell,mnc,centerlat,centerlon from
    (select split(key,'\\\\+')[0] as lac,split(key,'\\\\+')[1] as cell,split(key,'\\\\+')[2] as mnc,
    avg(centerlon) centerlon,avg(centerlat) centerlat,
    min(centerlon) centerlon_min,min(centerlat) centerlat_min,
    max(centerlon) centerlon_max,max(centerlat) centerlat_max
    from
      (select * from
        (select *,rank() over(partition by key order by poi_num desc) rn from
            (select key,cluster,centerlon,centerlat,count(1) as poi_num
            from $mid_dbscan_data_result_month
            where cluster!=0
            group by key,cluster,centerlon,centerlat
            )a
        )b
      where rn=1)c
    group by split(key,'\\\\+')[0],split(key,'\\\\+')[1],split(key,'\\\\+')[2]
    )d
where get_distance(centerlat_min,centerlon_min,centerlat_max,centerlon_max)<=4000;

-- 聚类的簇的平均距离大于2km

drop table if exists $mid_dbscan_data_final_month_step2;
create table $mid_dbscan_data_final_month_step2 stored as orc as
select lac,cell,mnc,centerlat,centerlon,country,province_code,city_code,area_code
from
    (select a.*,case when b.province_code rlike '^cn' then 'cn' else '' end as country,b.province_code, b.city_code, b.area_code
    from
    (
    select *, get_geohash(centerlat, centerlon, 6) geohash6, get_geohash(centerlat, centerlon, 7) geohash7
    from $mid_dbscan_data_final_month_step1
    ) as a
    left join
    (
    select geohash_6_code, province_code, city_code, area_code
    from $geohash6_area_mapping_par
    where version = '1000'
    ) as b
    on a.geohash6 = b.geohash_6_code
    where b.geohash_6_code is not null)t1
union all
select lac,cell,mnc,centerlat,centerlon,country,province_code,city_code,area_code
from
    (select e.*,case when f.province_code rlike '^cn' then 'cn' else ''end as country,f.province_code, f.city_code, f.area_code
    from
    (
    select c.* from
    (
        select *, get_geohash(centerlat, centerlon, 6) geohash6, get_geohash(centerlat, centerlon, 7) geohash7, get_geohash(centerlat, centerlon, 8) as geohash8
        from $mid_dbscan_data_final_month_step1
    ) as c
    left join
    (
        select geohash_6_code, province_code, city_code, area_code
        from $geohash6_area_mapping_par
        where version = '1000'
    ) as d
    on c.geohash6 = d.geohash_6_code
    where d.geohash_6_code is null
    ) as e
    left join
    (
    select geohash_8_code, province_code, city_code, area_code
    from $geohash8_lbs_info_mapping_par
    where version = '1000'
    ) as f
    on e.geohash8 = f.geohash_8_code)t2
;

"

## 初始生成使用
:<<!
HADOOP_USER_NAME=dba hive -v -e "
insert overwrite table $dim_base_dbscan_result_month partition(day='$day')
select mcc,mnc,lac,cell,lat,lon,acc,geohash8,addr,country,province,city,district,street,validity,carrier,network,'0' as flag_abnormal from
(select a.* from
(select *
from $mapping_base_station_location
where day='20190701')a
left outer join
(select * from $mid_dbscan_data_final_month_step3
where flag_abnormal=1)b
on a.lac=b.lac
and a.cell=b.cell
and a.mnc=b.mnc
where b.lac is null
)t1
union all
select * from
(select c.* from
(select 460 as mcc,mnc,lac,cell,centerlat as lat,centerlon as lon,'' as acc,get_geohash(centerlat,centerlon,8) as geohash8,'' as addr,
country,province_code as province,city_code as city,area_code as district,'' as street,'1' as validity,
case when mnc in (0,2,4,7,8) then '移动'
     when mnc in (1,6,9,10) then '联通'
     when mnc in (3,5,11,12) or mnc>=10000 then '电信'
     else '' end as carrier,'' as network,'0' as flag_abnormal
from $mid_dbscan_data_final_month_step2
where country='cn')c
left outer join
(select * from $mid_dbscan_data_final_month_step3
where flag_abnormal=0)d
on c.lac=d.lac
and c.cell=d.cell
and c.mnc=d.mnc
where d.lac is null
)t2;
"
!

HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

insert overwrite table $dim_base_dbscan_result_month partition(day='$day')
select mcc,mnc,lac,cell,lat,lon,acc,geohash8,addr,country,province,city,district,street,validity,carrier,network,flag_abnormal
from (
      select mcc,mnc,lac,cell,lat,lon,acc,geohash8,addr,country,province,city,district,street,validity,carrier,network,flag_abnormal,
      row_number() over(partition by mnc,lac,cell order by day desc) as rank
      from (
            select mcc,mnc,lac,cell,lat,lon,acc,geohash8,addr,country,province,city,district,street,validity,carrier,network,flag_abnormal, day
            from
             $dim_base_dbscan_result_month
             where day='$dim_base_dbscan_result_month_partition'
             union all
            select 460 as mcc,mnc,lac,cell,centerlat as lat,centerlon as lon,'' as acc,get_geohash(centerlat,centerlon,8) as geohash8,'' as addr,
            country,province_code as province,city_code as city,area_code as district,'' as street,'1' as validity,
            case when mnc in (0,2,4,7,8) then '移动'
                 when mnc in (1,6,9,10) then '联通'
                 when mnc in (3,5,11,12) or mnc>=10000 then '电信'
                 else '' end as carrier,'' as network,'0' as flag_abnormal,'$day' as day
            from $mid_dbscan_data_final_month_step2
            where country='cn'
            )t2
)f
where rank=1;
"