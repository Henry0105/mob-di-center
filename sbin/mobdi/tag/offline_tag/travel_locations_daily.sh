#!/bin/bash

set -x -e
if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

if [ -z "$1" ]; then 
  exit 1
fi

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#input
#dm_mobdi_topic.dws_device_location_current_di
#dm_mobdi_report.rp_device_location_permanent

#out
#dm_mobdi_topic.dws_device_travel_location_di
#dm_mobdi_report.ads_device_travel_di_old

insert_day=$1
hive_url='10.6.161.16'

# 第二个参数为非必须参数，如果指定，则显示指定了 rp_mobdi_app.rp_device_location_permanent 的分区，否则使用默认最新分区数据。
if [ -z "$2" ]; then 
    rp_device_location_permanent_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dm_mobdi_report', 'rp_device_location_permanent', 'day');
    drop temporary function GET_LAST_PARTITION;"
    rp_device_location_permanent_last_snapshot_day=(`hive -e "$rp_device_location_permanent_sql"`)
else
    rp_device_location_permanent_last_snapshot_day=$2
fi

echo "rp_device_location_permanent_last_snapshot_day=${rp_device_location_permanent_last_snapshot_day}"

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;
SET hive.merge.smallfiles.avgsize=256000000;
SET mapred.max.split.size=256000000;
SET mapred.min.split.size.per.node=128000000;
SET mapred.min.split.size.per.rack=128000000;

WITH location_current_pre as (
  SELECT
  device,
  location_map['country'] AS country,
  location_map['province'] AS province,
  location_map['city'] AS city,
  location_map['type'] AS type,
  plat
  FROM $dws_device_location_current_di
  LATERAL VIEW explode(location) t_loc as location_map
  WHERE day = '$insert_day'
)
,
location_current_count AS(
  SELECT current_pre.device,current_pre.country,current_pre.province,current_pre.city,current_pre.type,current_pre.plat,country_cnt.cnt
  FROM location_current_pre current_pre
  LEFT JOIN
  (SELECT device,count(*) as cnt
   FROM
   (
    select device,country from  location_current_pre GROUP BY device,country
   ) t
   GROUP BY device
  ) country_cnt
  ON current_pre.device=country_cnt.device
)

INSERT OVERWRITE TABLE $dws_device_travel_location_di partition (day = '$insert_day')
SELECT
location_current.device,
location_current.country,
location_current.province,
location_current.city,
location_permanent.country AS pcountry,
location_permanent.province AS pprovince,
location_permanent.city AS pcity,
location_current.plat
FROM
(
  SELECT device,country,province,city,plat
  FROM location_current_count
  WHERE cnt<3
  UNION ALL
  SELECT device,country,province,city,plat
  FROM location_current_count
  WHERE cnt>=3 and type <> 'ip'
) location_current
LEFT JOIN
(
  SELECT device, country, province, city
  FROM $rp_device_location_permanent
  WHERE day = '$rp_device_location_permanent_last_snapshot_day'
) location_permanent ON (location_permanent.device = location_current.device)
;

"
spark2-submit --master yarn \
            --deploy-mode cluster \
            --class youzu.mob.travel.TravelApp \
            --name TravelApp_$day \
            --conf spark.hadoop.validateOutputSpecs=false \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.minExecutors=1 \
            --conf spark.shuffle.service.enabled=true \
            --conf spark.dynamicAllocation.maxExecutors=30 \
            --conf spark.locality.wait=100ms \
            --conf spark.sql.codegen=true \
            --conf spark.network.timeout=300000 \
            --conf spark.core.connection.ack.wait.timeout=300000 \
            --conf spark.storage.blockManagerSlaveTimeoutMs=300000 \
            --conf spark.shuffle.io.connectionTimeout=300000 \
            --conf spark.rpc.askTimeout=3000 \
            --conf spark.rpc.lookupTimeout=300000 \
            --conf spark.sql.autoBroadcastJoinThreshold=31457280 \
            --conf spark.sql.orc.filterPushdown=true \
            --executor-memory 12g \
            --executor-cores 4 \
            --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=1024m -XX:PermSize=256m" \
	   /home/dba/lib/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar  "$insert_day" "$hive_url" "$ads_device_travel_di_old"

hive -v -e "
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.exec.parallel=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;

insert  overwrite table $ads_device_travel_di_old  partition (day='$insert_day')
select device,country,province,city,pcountry,pprovince,pcity,poi_flag int,continents,travel_area,province_flag,pcity_level,vaca_flag,business_flag,busi_app_act,car,travel_app_act,cheap_flight_installed,flight_installed,flight_active,ticket_installed,ticket_active,rentcar_active,rentcar_installed
from $ads_device_travel_di_old where day='$insert_day'
"