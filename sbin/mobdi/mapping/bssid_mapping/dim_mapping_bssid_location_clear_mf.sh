#/bin/bash

set -x -e

day=$1
nowdate=`date -d $day +%Y%m01`

#获取dm_mobdi_mapping.dim_mapping_bssid_location_mf 离day最近的3个分区

last_partition=`date -d "$day -1 month" +%Y%m26`
last_second_partition=`date -d "$last_partition -1 months" "+%Y%m%d"`
last_three_partition=`date -d "$last_partition -2 months" "+%Y%m%d"`

## input
dim_mapping_bssid_location_mf=dim_mobdi_mapping.dim_mapping_bssid_location_mf

## output
dim_mapping_bssid_location_clear_mf=dim_mobdi_mapping.dim_mapping_bssid_location_clear_mf


HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';

insert overwrite table $dim_mapping_bssid_location_clear_mf partition (day='$nowdate')
select t1.bssid,t1.lat,t1.lon,t1.acc,t1.geohash8,t1.addr,t1.country,t1.province,t1.city,t1.district,t1.street,t1.ssid,t1.bssid_type,
       case when t2.bssid is null and bssid_type=1 then 0
            else 1
       end as flag
from
(select bssid,lat,lon,acc,geohash8,addr,country,province,city,district,street,ssid,bssid_type
  from $dim_mapping_bssid_location_mf
  where day='${last_partition}'
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
on t1.bssid=t2.bssid;"
