#!/bin/bash

set -x -e

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

## input
#dim_mapping_bssid_location_mf=dm_mobdi_mapping.dim_mapping_bssid_location_mf

## output
#dim_mapping_bssid_location_clear_mf=dm_mobdi_mapping.dim_mapping_bssid_location_clear_mf

bssid_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'dim_mapping_bssid_location_mf', 'day');
"
last_partition=(`hive -e "$bssid_mapping_sql"`)


newestThreePartitions=`hive -e "show partitions $dim_mapping_bssid_location_mf" | awk -v day=${last_partition} -F '=' '$2<=day {print $0}'| sort| tail -n 3| xargs echo| sed 's/\s/,/g'`


last_second_partition=`echo $newestThreePartitions |cut -d"," -f2`

last_three_partition=`echo $newestThreePartitions |cut -d"," -f1`

hive -v -e"
insert overwrite table $dim_mapping_bssid_location_clear_mf partition (day='$day')
select t1.*,
       case when t2.bssid is null then 0
            else 1
       end as flag
from
(select *
  from $dim_mapping_bssid_location_mf
  where day='${last_partition}' and bssid_type=1
  ) t1
left join
(
select bssid
     from(
      select bssid
        from
          (select bssid,case when distance1>=distance2 then distance1 else distance2 end as distance from
              (select bssid,
               case when distance_1>=distance_2 then distance_1 else distance_2 end as distance1,
               case when distance_2>=distance_3 then distance_2 else distance_3 end as distance2
              from
                  (select bssid,
                  get_distance(lat_1,lon_1,lat_2,lon_2) as distance_1,
                  get_distance(lat_2,lon_2,lat_3,lon_3) as distance_2,
                  get_distance(lat_1,lon_1,lat_3,lon_3) as distance_3
                  from (
                    select a.bssid,a.lat_1,a.lon_1,b.lat_2,b.lon_2,c.lat_3,c.lon_3   -- 聚合最新三个分区数据
                          from
                          (
                             select bssid,lat as lat_1,lon as lon_1
                             from $dim_mapping_bssid_location_mf
                             where day='${last_partition}' and bssid_type=1
                          )a
                          left join
                          (
                             select bssid,lat as lat_2,lon as lon_2
                             from $dim_mapping_bssid_location_mf
                             where day='${last_second_partition}' and bssid_type=1
                          )b on a.bssid=b.bssid
                          left join
                          (
                             select bssid,lat as lat_3,lon as lon_3
                             from $dim_mapping_bssid_location_mf
                             where day='${last_three_partition}' and bssid_type=1
                          )c on a.bssid=c.bssid
                  )t1
                  where lat_1 is not null
                  and lat_2 is not null
                  and lat_3 is not null
                )t2
              )t3
          )t4
      where distance>500

      union all

      select bssid  -- 获取经纬度对应bssid小于3的所有bssid
      from(
      select bssid,lat,lon,count(1) over (partition by lat,lon) as bssid_cnt
      from
      $dim_mapping_bssid_location_mf
      where day='$last_partition' and bssid_type=1
      )t1
      where bssid_cnt>3
     )tt1
     group by bssid
) t2
on t1.bssid=t2.bssid;
"