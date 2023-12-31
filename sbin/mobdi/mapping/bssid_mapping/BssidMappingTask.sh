#!/bin/bash
: '
@owner:zhangxy || zhtli
@describe:$dim_mapping_bssid_location_mf这张表的更新方式
@projectName:mobdi
@BusinessName:BssidMapping
'

set -e -x

day=$1
pday=`date -d "$day -1 month" "+%Y%m%d"`
#导入配置文件
#ource /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
#source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#input
dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di
dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
bssid_exchange=dw_mobdi_source.bssid_exchange
#md
calculate_bssid_mapping_base_info=dm_mobdi_tmp.calculate_bssid_mapping_base_info
speed_abnormal_device_info_for_mapping=dm_mobdi_tmp.speed_abnormal_device_info_for_mapping
calculate_bssid_mapping_base_info_except_abnormal_data=dm_mobdi_tmp.calculate_bssid_mapping_base_info_except_abnormal_data
bssid_from_gps_par=dm_mobdi_tmp.bssid_from_gps_par
bssid_finaltable_gps_cnt1_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt1_par
bssid_finaltable_gps_cnt2_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt2_par
bssid_strangetable_gps_cnt2_par=dm_mobdi_tmp.bssid_strangetable_gps_cnt2_par
bssid_finaltable_gps_cnt3_pre_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_pre_par
bssid_finaltable_gps_cnt3_dbscan_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_dbscan_par
bssid_finaltable_gps_cnt3_dbscan_result_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_dbscan_result_par
bssid_finaltable_gps_cnt3_dbscan_unique_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_dbscan_unique_par
bssid_finaltable_gps_cnt3_pre_allinfo_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_pre_allinfo_par
bssid_finaltable_gps_cnt3_pre_allinfo_maxday_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_pre_allinfo_maxday_par
bssid_finaltable_gps_cnt3_pre_allinfo_maxday_maxcnt_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_pre_allinfo_maxday_maxcnt_par
bssid_finaltable_gps_cnt3_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_par
bssid_finaltable_gps_cnt3_notsure_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_notsure_par
bssid_finaltable_gps_cnt3_notsure_trans_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_notsure_trans_par
bssid_strangetable_gps_cnt3_sure_par=dm_mobdi_tmp.bssid_strangetable_gps_cnt3_sure_par
bssid_finaltable_gps_cnt3_sure_pre_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_sure_pre_par
bssid_finaltable_gps_cnt3_sure_par=dm_mobdi_tmp.bssid_finaltable_gps_cnt3_sure_par
bssid_strangetable_gps_cnt3_par=dm_mobdi_tmp.bssid_strangetable_gps_cnt3_par
bssid_strangetable_gps_par=dm_mobdi_tmp.bssid_strangetable_gps_par
bssid_finaltable_gps_par=dm_mobdi_tmp.bssid_finaltable_gps_par
bssid_finaltable_gps_new_par=dm_mobdi_tmp.bssid_finaltable_gps_new_par
bssid_finaltable_par=dm_mobdi_tmp.bssid_finaltable_par
bssid_finaltable_addgeohash8_par=dm_mobdi_tmp.bssid_finaltable_addgeohash8_par
bssid_finaltable_addgeohash8_addlocation_par=dm_mobdi_tmp.bssid_finaltable_addgeohash8_addlocation_par

#mapping
dim_geohash8_china_area_mapping_par=dm_sdk_mapping.geohash8_lbs_info_mapping_par
dim_geohash6_china_area_mapping_par=dm_sdk_mapping.geohash6_area_mapping_par
dim_bssid_type_mf=dim_mobdi_mapping.dim_bssid_type_mf

#output
dim_mapping_bssid_location_mf=dim_mobdi_mapping.dim_mapping_bssid_location_mf
dim_bssid_ssid_mapping_par=dim_mobdi_mapping.dim_bssid_ssid_mapping_par

echo "step 1:get gps data from log..."

#9月添加
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coord_convertor as 'com.youzu.mob.java.udf.CoordConvertor';
insert overwrite table $calculate_bssid_mapping_base_info partition(day='$day')
select deviceid, bssid, ssid, latitude, longitude, clienttime, accuracy, log_day
from
(
    select muid as deviceid, trim(lower(cur_bssid)) as bssid, cur_ssid as ssid, latitude, longitude, clienttime, accuracy, day as log_day
    from $dwd_location_info_sec_di
    where day >$pday and day <=$day
    and regexp_replace(trim(lower(cur_bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
    and cur_bssid is not null
    and latitude is not null
    and longitude is not null
    and trim(lower(cur_bssid)) not in ('00:00:00:00:00:00','02:00:00:00:00:00','01:80:c2:00:00:03','ff:ff:ff:ff:ff:ff','00:02:00:00:00:00')
    and abs(latitude) <= 90 and abs(longitude) <= 180 and (latitude <> 0 or longitude <> 0)
    and ((latitude - round(latitude, 1))*10 <> 0.0 and (longitude - round(longitude, 1))*10 <> 0.0)
    and plat=1
    group by muid, trim(lower(cur_bssid)), cur_ssid, latitude, longitude, clienttime, accuracy, day

    union all

    select deviceid, trim(lower(cur_bssid)) as bssid, cur_ssid as ssid, latitude, longitude, clienttime, accuracy, day as log_day
    from $dwd_location_info_sec_di
    where day >$pday and day <=$day
    and regexp_replace(trim(lower(cur_bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
    and cur_bssid is not null
    and latitude is not null
    and longitude is not null
    and trim(lower(cur_bssid)) not in ('00:00:00:00:00:00','02:00:00:00:00:00','01:80:c2:00:00:03','ff:ff:ff:ff:ff:ff','00:02:00:00:00:00')
    and abs(latitude) <= 90 and abs(longitude) <= 180 and (latitude <> 0 or longitude <> 0)
    and ((latitude - round(latitude, 1))*10 <> 0.0 and (longitude - round(longitude, 1))*10 <> 0.0)
    and plat=2
    group by deviceid, trim(lower(cur_bssid)), cur_ssid, latitude, longitude, clienttime, accuracy, day

    union all

    select device as deviceid, bssid, ssid, latitude, longitude, datetime as clienttime, accuracy, day as log_day
    from
    (
      select muid as device, trim(lower(bssid)) as bssid, ssid, datetime,day,
            split(coord_convertor(cl['latitude'],cl['longitude'],'wsg84','bd09'),',')[0] latitude,
            split(coord_convertor(cl['latitude'],cl['longitude'],'wsg84','bd09'),',')[1] longitude,
            cl['accuracy'] accuracy,
            cl['ltime'] ltime
      from $dwd_log_wifi_info_sec_di
      where day >$pday and day <=$day
	    and cl['latitude'] is not null
      and cl['longitude'] is not null
      and abs(cl['latitude']) <= 90 and abs(cl['longitude']) <= 180 and (cl['latitude'] <> 0 or cl['longitude'] <> 0)
      and ((cl['latitude'] - round(cl['latitude'], 1))*10 <> 0.0 and (cl['longitude'] - round(cl['longitude'], 1))*10 <> 0.0)
      and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
      and bssid is not null
      and trim(lower(bssid)) not in ('00:00:00:00:00:00','02:00:00:00:00:00','01:80:c2:00:00:03','ff:ff:ff:ff:ff:ff','00:02:00:00:00:00')
      and datetime-cl['ltime']<=300000
      and plat=1

      union all

            select device, trim(lower(bssid)) as bssid, ssid, datetime,day,
            split(coord_convertor(cl['latitude'],cl['longitude'],'wsg84','bd09'),',')[0] latitude,
            split(coord_convertor(cl['latitude'],cl['longitude'],'wsg84','bd09'),',')[1] longitude,
            cl['accuracy'] accuracy,
            cl['ltime'] ltime
      from $dwd_log_wifi_info_sec_di
      where day >$pday and day <=$day
	    and cl['latitude'] is not null
      and cl['longitude'] is not null
      and abs(cl['latitude']) <= 90 and abs(cl['longitude']) <= 180 and (cl['latitude'] <> 0 or cl['longitude'] <> 0)
      and ((cl['latitude'] - round(cl['latitude'], 1))*10 <> 0.0 and (cl['longitude'] - round(cl['longitude'], 1))*10 <> 0.0)
      and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
      and bssid is not null
      and trim(lower(bssid)) not in ('00:00:00:00:00:00','02:00:00:00:00:00','01:80:c2:00:00:03','ff:ff:ff:ff:ff:ff','00:02:00:00:00:00')
      and datetime-cl['ltime']<=300000
      and plat=2
    ) a
    where latitude is not null
    and longitude is not null
    and abs(latitude) <= 90 and abs(longitude) <= 180 and (latitude <> 0 or longitude <> 0)
    and ((latitude - round(latitude, 1))*10 <> 0.0 and (longitude - round(longitude, 1))*10 <> 0.0)
    group by device, bssid, ssid, latitude, longitude, datetime, accuracy, day

) b group by deviceid, bssid, ssid, latitude, longitude, clienttime, accuracy, log_day
"
#12小时内的移动距离>=100m且平均速度>=30m/s，认为是异常数据
HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $speed_abnormal_device_info_for_mapping partition(day='$day')
select deviceid, longitude, latitude, longitude_new, latitude_new, distance, time
from
(
  select deviceid, longitude, latitude, longitude_new, latitude_new,
         cast(get_distance(latitude,longitude,latitude_new,longitude_new) as int) as distance,
         if(clienttime-clienttime_new=0,0.0001,clienttime-clienttime_new) as time
  from
  (
    select deviceid, longitude, latitude, clienttime,
           lag(longitude,1) over(partition by deviceid order by clienttime) as longitude_new,
           lag(latitude,1) over(partition by deviceid order by clienttime) as latitude_new,
           lag(clienttime,1,0) over(partition by deviceid order by clienttime) as clienttime_new
    from $calculate_bssid_mapping_base_info
    where day='$day'
  ) t1
  where latitude_new is not null
  and longitude_new is not null
  and clienttime-clienttime_new<=43200000
) t2
where cast(nvl(distance/time*1000,0) as bigint)>=30
and distance>=100;

--异常数据出现超过两次以上的认为是缓存数据，需要剔除
insert overwrite table $calculate_bssid_mapping_base_info_except_abnormal_data partition(day='$day')
select t1.deviceid, bssid, ssid, t1.latitude, t1.longitude, clienttime, accuracy, log_day
from $calculate_bssid_mapping_base_info t1
left join
(
  select deviceid, longitude, latitude
  from
  (
    select deviceid, longitude, latitude
    from $speed_abnormal_device_info_for_mapping
    where day='$day'

    union all

    select deviceid, longitude_new as longitude, latitude_new as latitude
    from $speed_abnormal_device_info_for_mapping
    where day='$day'
  ) t1
  group by deviceid, longitude, latitude
  having count(1)>=2
) t2 on t1.deviceid=t2.deviceid and t1.latitude=t2.latitude and t1.longitude=t2.longitude
where t1.day='$day'
and t2.deviceid is null;
"

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $bssid_from_gps_par partition(dt=$day)
select device, bssid, lat, lon, day_hour as day, acc_set, count(*) over(partition by bssid) as cnt, ssid_set
from
(
  select deviceid as device, bssid, latitude as lat, longitude as lon,
         collect_list(cast(accuracy as string)) as acc_set,
         collect_set(ssid) as ssid_set,
         from_unixtime(cast(clienttime/1000 as bigint),'yyyyMMdd HH') as day_hour
  from $calculate_bssid_mapping_base_info_except_abnormal_data
  where day='$day'
  group by deviceid, bssid, latitude, longitude, from_unixtime(cast(clienttime/1000 as bigint),'yyyyMMdd HH')
) tmp;
"

echo "get data from gps over"

#step 2:取cnt = 1的bssid直接放入最终表

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt1_par partition(dt=$day)
select bssid, lat, lon, acc_set, ssid_set
from $bssid_from_gps_par
where dt='$day'
and cnt = 1;
"

data_2_pre_sql="
select bssid,
       collect_list(lat) as latlist,
       collect_list(lon) as lonlist,
       collect_list(concat_ws(',',acc_set)) as acc_set,
       collect_set(concat_ws(',', ssid_set)) as ssid_set
from
(
  select bssid, lat, lon, acc_set, ssid_set
  from $bssid_from_gps_par
  where dt='$day'
  and cnt = 2
) cnt2
group by bssid"


HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit --master yarn --deploy-mode cluster \
--queue root.yarn_data_compliance \
--class com.youzu.mob.bssidmapping.bssidGpsCnt2 \
--driver-memory 10G \
--executor-memory 12G \
--executor-cores 2 \
--name "muid_bassid_gps_cnt2_task_par" \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=45 \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=7168 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=3000 \
--conf spark.sql.shuffle.partitions=4000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4 \
--conf spark.akka.timeout=600 \
--conf spark.network.timeout=600 \
--driver-java-options "-XX:MaxPermSize=1g" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar "$data_2_pre_sql" $bssid_finaltable_gps_cnt2_par $bssid_strangetable_gps_cnt2_par $day

# 合并小文件
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
insert overwrite table $bssid_finaltable_gps_cnt2_par partition(dt='$day')
select bssid,lat,lon,acc_set,ssid_set
from $bssid_finaltable_gps_cnt2_par
where dt='$day';

insert overwrite table $bssid_strangetable_gps_cnt2_par partition(dt='$day')
select bssid,latlist,lonlist,acc_set,ssid_set
from $bssid_strangetable_gps_cnt2_par
where dt='$day';
"

echo "step 4: compute gps_cnt3 tables start..."

HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_pre_par partition(dt=$day)
select bssid, lat, lon, day, acc_set, ssid_set
from $bssid_from_gps_par
where dt='$day'
and cnt >= 3
and cnt < 10000

union all

select bssid, lat, lon, day, acc_set, ssid_set
from
(
  select bssid, lat, lon, day, acc_set, ssid_set,
         row_number() over(partition by bssid order by day desc) as rank
  from $bssid_from_gps_par
  where dt='$day'
  and cnt >= 10000
) t1
where rank<10000;
"

cnt3_pre_sql="
select bssid, lat, lon
from $bssid_finaltable_gps_cnt3_pre_par
where dt='$day'
"


HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.mydbscan.DBSCAN_gps_cnt3 \
--queue root.yarn_data_compliance \
--driver-memory 10G \
--executor-memory 12G \
--executor-cores 2 \
--name "muid_bassid_gps_cnt3_dbscan_task_$day" \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=45 \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=7168 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=5000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.2 \
--conf spark.shuffle.memoryFraction=0.6 \
--conf spark.akka.timeout=600 \
--conf spark.network.timeout=600 \
--driver-java-options "-XX:MaxPermSize=1g" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar "$cnt3_pre_sql" $bssid_finaltable_gps_cnt3_dbscan_par $day

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

insert overwrite table $bssid_finaltable_gps_cnt3_dbscan_par partition(dt=$day)
select bssid,lon,lat,cluster,centerlon,centerlat
from $bssid_finaltable_gps_cnt3_dbscan_par
where dt='$day'
"

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_dbscan_result_par partition(dt=$day)
select bssid, lat, lon, cnt
from
(
  select bssid, lat, lon, count(*) as cnt
  from
  (
    select bssid, lat, lon, cluster
    from $bssid_finaltable_gps_cnt3_dbscan_par
    where dt='$day'
    group by bssid, lat, lon, cluster
  ) as a
  group by bssid, lat, lon
) as b
where cnt > 1;
"

#--1.将这批异常点从聚类结果中删除,2.对清洗之后的聚类结果进行去重
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_dbscan_unique_par partition(dt=$day)
select bssid, lat, lon, cluster, centerlon, centerlat
from
(
  select a.bssid, a.lat, a.lon, a.cluster, a.centerlon, a.centerlat
  from $bssid_finaltable_gps_cnt3_dbscan_par as a
  left join
  $bssid_finaltable_gps_cnt3_dbscan_result_par as b
  on b.dt='$day' and a.bssid = b.bssid and a.lat = b.lat and a.lon = b.lon
  where a.dt='$day'
  and b.bssid is null
) dbscan_clean
group by bssid, lat, lon, cluster, centerlon, centerlat;
"

#--将聚类结果匹配回
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_pre_allinfo_par partition(dt=$day)
select bssid, lat, lon, day, acc_set, cluster, centerlon, centerlat,
       count(*) over(partition by bssid, cluster) as cnt,
       ssid_set
from
(
  select a.bssid, a.lat, a.lon, a.day, a.acc_set, a.ssid_set, b.cluster, b.centerlon, b.centerlat
  from $bssid_finaltable_gps_cnt3_pre_par as a
  left join
  $bssid_finaltable_gps_cnt3_dbscan_unique_par as b
  on b.dt='$day' and a.bssid = b.bssid and a.lat = b.lat and a.lon = b.lon
  where a.dt='$day'
) tmp;
"

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_par partition(dt=$day)
select a.bssid, a.cluster, a.cnt, a.day
from
(
  select bssid, cluster, cnt, day
  from $bssid_finaltable_gps_cnt3_pre_allinfo_par
  where dt='$day'
  and cluster <> 0
  and cluster is not null
  group by bssid, cluster, cnt, day
) as a
inner join
(
  select bssid, max(day) as day
  from $bssid_finaltable_gps_cnt3_pre_allinfo_par
  where dt='$day'
  and cluster <> 0
  and cluster is not null
  group by bssid
) as b on a.bssid = b.bssid and a.day = b.day;

insert overwrite table $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_maxcnt_par partition(dt=$day)
select bssid, cluster, cnt, day,
       count(*) over(partition by bssid) as cnt_cluster
from
(
  select bssid, cluster, cnt, day
  from
  (
    select a.bssid, a.cluster, a.cnt, a.day
    from $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_par as a
    inner join
    (
      select bssid, max(cnt) as cnt
      from $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_par
      where dt='$day'
      group by bssid
    ) as b on a.bssid = b.bssid and a.cnt = b.cnt
    where a.dt='$day'
  ) as d
  group by bssid, cluster, cnt, day
) tmp;
"

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_par partition(dt=$day)
select bssid, lat, lon,
       collect_list(concat_ws(',', acc_set)) as acc_set,
       collect_set(concat_ws(',', ssid_set)) as ssid_set
from
(
  select a.bssid, a.centerlat as lat, a.centerlon as lon, a.acc_set, a.ssid_set
  from $bssid_finaltable_gps_cnt3_pre_allinfo_par as a
  inner join
  (
    select bssid, cluster
    from $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_maxcnt_par
    where dt='$day'
    and cnt_cluster = 1
  ) as b on a.bssid = b.bssid and a.cluster = b.cluster
  where a.dt='$day'
) as d
group by bssid, lat, lon;
"

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_finaltable_gps_cnt3_notsure_par partition(dt=$day)
select a.bssid, a.lat, a.lon, a.cluster
from $bssid_finaltable_gps_cnt3_pre_allinfo_par as a
inner join
(
  select bssid, cluster
  from $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_maxcnt_par
  where dt='$day'
  and cnt_cluster = 2
) as b on a.bssid = b.bssid and a.cluster = b.cluster
where a.dt='$day';

insert overwrite table $bssid_finaltable_gps_cnt3_notsure_trans_par partition(dt=$day)
select bssid, cluster, collect_list(lon) as lonlist, collect_list(lat) as latlist
from $bssid_finaltable_gps_cnt3_notsure_par
where dt='$day'
group by bssid, cluster;
"

cnt3_notsure_trans_pre_sql="
select bssid, latlist, lonlist, cluster
from $bssid_finaltable_gps_cnt3_notsure_trans_par
where dt='$day'"

HADOOP_USER_NAME=dba /opt/mobdata/sbin/spark-submit --master yarn --deploy-mode cluster \
--class com.youzu.mob.bssidmapping.bssidGpsCnt3NotsureMindistance \
--driver-memory 10G \
--queue root.yarn_data_compliance \
--executor-memory 12G \
--executor-cores 2 \
--name "muid_bssid_gps_cnt3_mindistance_$day" \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=45 \
--conf spark.dynamicAllocation.maxExecutors=80 \
--conf spark.dynamicAllocation.executorIdleTimeout=15s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.yarn.executor.memoryOverhead=7168 \
--conf spark.kryoserializer.buffer.max=1024 \
--conf spark.default.parallelism=2000 \
--conf spark.sql.shuffle.partitions=5000 \
--conf spark.driver.maxResultSize=4g \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4 \
--conf spark.akka.timeout=600 \
--conf spark.network.timeout=600 \
--driver-java-options "-XX:MaxPermSize=1g" \
/home/dba/mobdi_center/lib/MobDI-center-spark2-1.0-SNAPSHOT.jar "$cnt3_notsure_trans_pre_sql" $bssid_finaltable_gps_cnt3_notsure_par $bssid_strangetable_gps_cnt3_sure_par $bssid_finaltable_gps_cnt3_sure_pre_par $bssid_finaltable_gps_cnt3_pre_allinfo_par $day

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
insert overwrite table $bssid_strangetable_gps_cnt3_sure_par partition(dt=$day)
select bssid,latlist,lonlist,acc_set,ssid_set
from $bssid_strangetable_gps_cnt3_sure_par
where dt='$day';

insert overwrite table $bssid_finaltable_gps_cnt3_sure_pre_par partition(dt=$day)
select bssid,lon,lat,centerlon,centerlat
from $bssid_finaltable_gps_cnt3_sure_pre_par
where dt='$day';
"

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.parallel=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
insert overwrite table $bssid_finaltable_gps_cnt3_sure_par partition(dt=$day)
select bssid, lat, lon,
       collect_list(concat_ws(',', acc_set)) as acc_set,
       collect_set(concat_ws(',', ssid_set)) as ssid_set
from
(
  select a.bssid, b.centerlat as lat, b.centerlon as lon, a.acc_set, a.ssid_set
  from $bssid_finaltable_gps_cnt3_pre_allinfo_par as a
  inner join
  $bssid_finaltable_gps_cnt3_sure_pre_par as b
  on b.dt='$day' and a.bssid = b.bssid and a.lat = b.lat and a.lon = b.lon
  where a.dt='$day'
) as d
group by bssid, lat, lon;
"

#--对于其中cnt >= 3的bssid，归入异常表
#--对于聚类结果全是异常点的bssid，归入异常表
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_strangetable_gps_cnt3_par partition(dt=$day)
select bssid,
       collect_list(lat) as latlist,
       collect_list(lon) as lonlist,
       collect_list(concat_ws(',', acc_set)) as acc_set,
       collect_set(concat_ws(',', ssid_set)) as ssid_set
from
(
  select a.bssid, a.lat, a.lon, a.acc_set, a.ssid_set
  from $bssid_finaltable_gps_cnt3_pre_allinfo_par as a
  inner join
  (
    select bssid
    from $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_maxcnt_par
    where dt='$day'
    and cnt_cluster >= 3
    group by bssid
  ) as b on a.bssid = b.bssid
  where a.dt='$day'
) as d
group by bssid

union all

select bssid,
       collect_list(lat) as latlist,
       collect_list(lon) as lonlist,
       collect_list(concat_ws(',', acc_set)) as acc_set,
       collect_set(concat_ws(',', ssid_set)) as ssid_set
from
(
  select c.bssid, c.lat, c.lon, c.acc_set, c.ssid_set
  from $bssid_finaltable_gps_cnt3_pre_allinfo_par as c
  left join
  (
    select bssid
    from $bssid_finaltable_gps_cnt3_pre_allinfo_maxday_par
    where dt='$day'
    group by bssid
  ) d on c.bssid = d.bssid
  where c.dt='$day'
  and d.bssid is null
) as e
group by bssid;
"

#step4:
#--选择cnt >= 10000的bssid，可以用geohash方法或者聚类方法，反正是单独计算
:<<!
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table dw_mobdi_md.bssid_finaltable_gps_cnt3_pre_verybig_par partition(dt=$day)
select bssid, lat, lon, day, acc_set, ssid_set
from dw_mobdi_md.bssid_from_gps_par
where dt='$day'
and cnt >= 10000;"
!

#--合并所有异常表
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
insert overwrite table $bssid_strangetable_gps_par partition(dt=$day)
select bssid,latlist, lonlist, acc_set, day, ssid_set, confidence
from
(
  select bssid, latlist, lonlist, acc_set, ssid_set, day, confidence,
         row_number() over(partition by bssid order by day desc ) rn
  from
  (
    select bssid, latlist, lonlist, acc_set, ssid_set, $day as day, 0.1 as confidence
    from $bssid_strangetable_gps_cnt2_par
    where dt='$day'

    union all

    select bssid, latlist, lonlist, acc_set, ssid_set, $day as day, 0.3 as confidence
    from $bssid_strangetable_gps_cnt3_par
    where dt='$day'

    union all

    select bssid, latlist, lonlist, acc_set, ssid_set, $day as day, 0.2 as confidence
    from $bssid_strangetable_gps_cnt3_sure_par
    where dt='$day'
  ) aa
) tmp
where rn=1
"

#--合并所有正常表
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $bssid_finaltable_gps_par partition(dt=$day)
select bssid, lat, lon, acc_set, ssid_set, 1 as confidence
from $bssid_finaltable_gps_cnt1_par
where dt='$day'

union all

select bssid, lat, lon, acc_set, ssid_set, 1 as confidence
from $bssid_finaltable_gps_cnt2_par
where dt='$day'

union all

select bssid, lat, lon, acc_set, ssid_set, 1 as confidence
from $bssid_finaltable_gps_cnt3_par
where dt='$day'

union all

select bssid, lat, lon, acc_set, ssid_set, 1 as confidence
from $bssid_finaltable_gps_cnt3_sure_par
where dt='$day'

union all

select bssid, latlist[0] as lat, lonlist[0] as lon, acc_set, ssid_set, confidence
from $bssid_strangetable_gps_par
where dt='$day';
"

#--把正常表中的acc_set取均值，用于最终所有数据源的汇总
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx7300m';
set mapreduce.child.map.java.opts='-Xmx8192m';
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
set dfs.socket.timeout=3600000;
set dfs.datanode.socket.write.timeout=3600000;
insert overwrite table $bssid_finaltable_gps_new_par partition(dt=$day)
select bssid, lat, lon,
       avg(acc) as acc,
       collect_set(concat_ws(',', ssid_set)) as ssid,
       max(confidence) as confidence
from
(
  select bssid, lat, lon, cast(acc_split as double) as acc, ssid_set, confidence
  from $bssid_finaltable_gps_par
  lateral view explode(acc_set) mytable as acc_split
  where dt='$day'
) as a
group by bssid, lat, lon;
"

#--gps正常数据，声牙，wifipix，采买数据整合到一起
#--声牙的数据3个表合并之后依然保持bssid与经纬度的一一对应，算过了
extLastPartStr=`hive -e "show partitions $bssid_exchange" | sort | tail -n 1`

if [ -z "$extLastPartStr" ]; then
    extLastPartStrA=$extLastPartStr
fi

if [ -n "$extLastPartStr" ]; then
    extLastPartStrA=" AND $extLastPartStr"
fi

HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coord_convertor as 'com.youzu.mob.java.udf.CoordConvertor';
insert overwrite table $bssid_finaltable_par partition(dt=$day)
select bssid, lat, lon, acc, addr, street, ssid, confidence
from
(
  select bssid, lat, lon, acc, ssid, addr, street,
         row_number() over(partition by bssid order by flag) as num,
         confidence
  from
  (
    select bssid,
           split(transcoord, ',')[0] as lat,
           split(transcoord, ',')[1] as lon,
           acc, ssid, addr, street, flag, confidence
    from
    (
      select bssid, lat, lon, acc, concat_ws(',', ssid) as ssid, '' as addr, '' as street, 1 as flag,
             coord_convertor(lat, lon, 'wsg84', 'bd09') as transcoord,
             confidence
      from $bssid_finaltable_gps_new_par
      where dt='$day'
    ) as d

    union all

    select lower(bssid) as bssid, lat, lon, acc, ssid, addr, street, flag, 1 as confidence
    from $bssid_exchange
    where 1=1 $extLastPartStrA
  ) as a
) as b
where num = 1;
"

#--生成geohash8（因为晓东的工具需要geohash这个字段，所以先生成geohash8）
HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.1-SNAPSHOT.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
insert overwrite table $bssid_finaltable_addgeohash8_par partition(dt=$day)
select bssid, lat, lon, acc, addr, street, get_geohash(lat, lon, 8) as geohash8, ssid, confidence
from $bssid_finaltable_par
where dt='$day';

insert overwrite table $bssid_finaltable_addgeohash8_addlocation_par partition(dt=$day)
select a.bssid,
       cast(cast(a.lat as decimal(20, 6)) as string) as lat,
       cast(cast(a.lon as decimal(20, 6)) as string) as lon,
       cast(cast(a.acc as decimal(20, 2)) as string) as acc,
       case
         when a.addr = '[]' or a.addr = 'unknown' or a.addr = 'null' or a.addr is null or a.addr = 'NULL' then ''
         else a.addr
       end as addr, 
       case
         when a.street = '[]' or a.street = 'unknown' or a.street = 'null' or a.street is null or a.street = 'NULL' then ''
         else a.street
       end as street, 
       a.geohash8, 
       case
         when b.province_code rlike '^cn' then 'cn'
         else ''
       end as country, 
       case
         when b.province_code = '[]' or b.province_code = 'unknown' or b.province_code = 'null' or b.province_code is null or b.province_code = 'NULL'
         then ''
         else b.province_code
       end as province, 
       case
         when b.city_code = '[]' or b.city_code = 'unknown' or b.city_code = 'null' or b.city_code is null or b.city_code = 'NULL'
         then ''
         else b.city_code
       end as city, 
       case
         when b.area_code = '[]' or b.area_code = 'unknown' or b.area_code = 'null' or b.area_code is null or b.area_code = 'NULL'
         then ''
         else b.area_code
       end as district,
       a.ssid, confidence
from $bssid_finaltable_addgeohash8_par as a
left join
(
  select *
  from $dim_geohash6_china_area_mapping_par
  where version='1000'
) as b on substring(a.geohash8, 1, 6) = b.geohash_6_code
where a.dt='$day'
and b.geohash_6_code is not null

union all

select e.bssid,
       cast(cast(e.lat as decimal(20, 6)) as string) as lat,
       cast(cast(e.lon as decimal(20, 6)) as string) as lon,
       cast(cast(e.acc as decimal(20, 2)) as string) as acc,
       case
         when e.addr = '[]' or e.addr = 'unknown' or e.addr = 'null' or e.addr is null or e.addr = 'NULL' then ''
         else e.addr
       end as addr, 
       case
         when e.street = '[]' or e.street = 'unknown' or e.street = 'null' or e.street is null or e.street = 'NULL' then ''
         else e.street
       end as street, 
       e.geohash8, 
       case
         when f.province_code rlike '^cn' then 'cn'
         else ''
       end as country, 
       case
         when f.province_code = '[]' or f.province_code = 'unknown' or f.province_code = 'null' or f.province_code is null or f.province_code = 'NULL'
         then ''
         else f.province_code
       end as province, 
       case
         when f.city_code = '[]' or f.city_code = 'unknown' or f.city_code = 'null' or f.city_code is null or f.city_code = 'NULL'
         then ''
         else f.city_code
       end as city, 
       case
         when f.area_code = '[]' or f.area_code = 'unknown' or f.area_code = 'null' or f.area_code is null or f.area_code = 'NULL'
         then ''
         else f.area_code
       end as district,
       e.ssid, confidence
from
(
  select c.bssid, c.lat, c.lon, c.acc, c.ssid, c.addr, c.street, c.geohash8, confidence
  from $bssid_finaltable_addgeohash8_par as c
  left join
  (
    select *
    from $dim_geohash6_china_area_mapping_par
    where version='1000'
  ) as d on substring(c.geohash8, 1, 6) = d.geohash_6_code
  where c.dt='$day'
  and d.geohash_6_code is null
) as e
left join
(
  select *
  from $dim_geohash8_china_area_mapping_par
  where version='1000'
) as f
on e.geohash8 = f.geohash_8_code;
"

lastPartStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`
if [ -z "$lastPartStr" ]; then
    lastPartStrA=$lastPartStr
fi

if [ -n "$lastPartStr" ]; then
    lastPartStrA=" AND $lastPartStr"
fi
#bssidTypeAllSql="
#    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
#    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
#    SELECT GET_LAST_PARTITION('dim_mobdi_mapping', 'dim_bssid_type_mf', 'day');
#"
#bssidTypeAllPartition=(`hive -e "$bssidTypeAllSql"`)


if [ ${day:6:8} -ge 26 ]; then
    bssidTypeAllPartition=${day:0:6}26
fi

if [ ${day:6:8} -lt 26 ]; then
    bssidTypeAllPartition=`date -d "$day -1 month" "+%Y%m26"`
fi

if [ ${day} -le '20180326' ]; then
    bssidTypeAllPartition='20180326'
fi

HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function array_trim as 'com.youzu.mob.java.udf.ArrayTrimDistinct';

insert overwrite table $dim_mapping_bssid_location_mf partition(day =$day)
select t1.bssid, lat, lon, acc, geohash8, addr, country, province, city, district, street, ssid, nvl(t2.type,0) as bssid_type, confidence
from
(
    select bssid, lat, lon, acc, concat_ws(',',array_trim(split(ssid,','))) as ssid, geohash8, addr, country, province, city, district, street, confidence
    from
    (
      select bssid,
             case
               when lat > 90 then 90 when lat < -90 then -90
               else lat
             end as lat,
             case
               when lon > 180 then 180 when lat < -180 then -180
               else lon
             end as lon,
             acc, ssid, geohash8, addr, country, province, city, district, street, confidence
      from
      (
        select bssid, lat, lon, acc, ssid, geohash8, addr, country, province, city, district, street, confidence,
               row_number() over(partition by bssid order by day desc) as num
        from
        (
          select lower(bssid) as bssid, lat, lon, acc, ssid, geohash8, addr, country, province, city, district, street, day, confidence
          from $dim_mapping_bssid_location_mf
          where 1=1 $lastPartStrA

          union all

          select lower(bssid) as bssid, lat, lon, acc, ssid, geohash8, addr, country, province, city, district, street, '$day' as day, confidence
          from $bssid_finaltable_addgeohash8_addlocation_par
          where dt='$day'
        ) as a
      ) as b
      where num = 1
      and abs(lat) < 90.1 and abs(lon) < 180.1
      and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
    ) as un
) t1
left join
$dim_bssid_type_mf t2 on t2.day='$bssidTypeAllPartition' and t1.bssid=t2.bssid
;
"

HADOOP_USER_NAME=dba hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.min.split.size=200000000;
set mapred.max.split.size=300000000;
set mapred.min.split.size.per.node=50000000;
set mapred.min.split.size.per.rack=50000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.job.reuse.jvm.num.tasks=25;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
insert overwrite table $dim_bssid_ssid_mapping_par partition (day = $day)
select bssid, ssid, real_date
from
(
  select bssid, ssid, real_date,
         row_number() over(partition by bssid order by real_date desc) as num
  from
  (
    select bssid, ssid, real_date
    from $dim_bssid_ssid_mapping_par
    where day=GET_LAST_PARTITION('dim_mobdi_mapping', 'dim_bssid_ssid_mapping_par', 'day')

    union all

    select bssid,ssid,real_date from
    (
    select bssid, ssid, real_date,row_number() over(partition by bssid order by real_date desc) as num
    from
    (
      select bssid, ssid, from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss') as real_date
      from $dwd_log_wifi_info_sec_di
      where day > '$pday' and day <= '$day'
      and ssid not in ('', 'null', 'NULL', 'unknown', 'None', 'NONE')
      and ssid is not null
      and bssid not in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
      and bssid is not null
      and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
      group by bssid,ssid,from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd HH:mm:ss')
    ) as a
    where real_date >= '20170101'
    and real_date <= '$day'
    )t where num=1
  ) as b
) as c
where num = 1;
"

