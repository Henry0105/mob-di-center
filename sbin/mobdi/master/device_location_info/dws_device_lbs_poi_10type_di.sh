#!/bin/sh
set -x -e

: '
@owner:xdzhang
@describe:处理lbs数据，生成对应的poi数据
@projectName:MOBDI
@BusinessName:lbs_poi
@SourceTable:dm_sdk_mapping.catering_cate_mapping,dm_mobdi_master.device_staying_daily,dm_mobdi_mapping.lat_lon_poi_mapping
@TargetTable:dm_mobdi_mapping.lat_lon_poi_mapping
@TableRelation:ext_ad_dmp.dw_base_poi_l1_geohash,dm_mobdi_master.sdk_lbs_daily->dm_mobdi_mapping.lat_lon_poi_mapping|dm_mobdi_master.sdk_lbs_daily,dm_mobdi_mapping.lat_lon_poi_mapping->dm_mobdi_master.device_lbs_poi_daily|dm_mobdi_master.device_lbs_poi_daily,dm_sdk_mapping.catering_cate_mapping->dm_mobdi_master.device_catering_dinein_detail
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi


: '
@parameters
@day:传入日期参数，为脚本运行日期（重跑不同）
@beforeNum:运行数据区间（用来补数据或一起跑出多天数据时用）
@repartition:spark执行时自定义的分区数
@isHbase：请求poi标签时是否使用hbase，true为使用
'

b_time=$(date +%s)
day=$1
beforeNum=0
partitions=6000
isHbase=true
filesnum=300

: '
@part_1:
实现功能：找到lbs标签数据中每个设备的POI信息与餐饮店铺信息
实现逻辑：1.从dm_mobdi_master.device_staying_daily中取当日和前一日分区的数据, 与dm_mobdi_mapping.lat_lon_poi_mapping进行left join, 找出不在后一张表的经纬度数据
          2.步骤1中的经纬度数据调用LbsTypedBasedOnHBasePOIHandler.lbsPoiJoin方法, 去hbase查找相应的poi数据
          3.步骤1的数据与步骤2的数据进行left join, 结果存入dw_mobdi_md.udf_tmp
          4.dm_mobdi_master.device_staying_daily与dw_mobdi_md.udf_tmp的数据加上dm_mobdi_mapping.lat_lon_poi_mapping关联
          5.步骤4的数据存入dm_mobdi_master.device_lbs_poi_daily, 得到设备地理位置对应的各类型poi表
          6.步骤4的数据先依据time_start划分早餐、早午餐、午餐、下午茶、晚餐、夜宵, 然后与餐饮店铺的mapping表(dm_sdk_mapping.catering_cate_mapping)
            关联得到一个device一天中堂吃信息, 保存到dm_mobdi_master.device_catering_dinein_detail
          7.dw_mobdi_md.udf_tmp加上dm_mobdi_mapping.lat_lon_poi_mapping, 结果存入dm_mobdi_mapping.lat_lon_poi_mapping
输出结果：device_lbs_poi_daily
    字段：device
          lat
          lon
          time_start
          time_end
          type_1 至 type_10 所在经纬度附近所有的店铺类型
          province
          type
          city
          area
          street
          plat

       表 device_catering_dinein_detail
    字段：device
          brand  品牌
          taste  口味
          type1
          type2
          time   堂食时间
'
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di
#dim_lat_lon_poi_mapping=dim_mobdi_mapping.dim_lat_lon_poi_mapping
#dim_catering_cate_mapping=dim_sdk_mapping.dim_catering_cate_mapping
dw_base_poi_l1_geohash=$dm_mobdi_tmp.dw_base_poi_l1_geohash

#out spark生成
udf_tmp=$dm_mobdi_tmp.udf_tmp
#dm_mobdi_topic.dws_device_catering_dinein_di
#dm_mobdi_topic.dws_device_lbs_poi_10type_di



bday=`date -d "$day -$((${beforeNum}+1)) days" "+%Y%m%d"`
dropsql=""
lbs_tmp="
select *
from $dws_device_location_staying_di s
where s.day<= ${day}
and day >${bday}
and s.type in ('base','wifi')
and ga_abnormal_flag = 0
"
poi_tmp="
select geohash_center5,geohash_center6,geohash_center7,geohash_center8,lat,lon,
       type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10
from $dim_lat_lon_poi_mapping
"
tmp_sql="
select s.device as device,s.lat,s.lon,s.start_time as time_start, s.end_time as time_end,
       u.type_1,u.type_2,u.type_3,u.type_4 as type_4,u.type_5,u.type_6,u.type_7,u.type_8,u.type_9,u.type_10,
       s.province, s.type, s.city, s.area, s.street, s.plat
from
(
  select *
  from sdk_lbs_tmp_muid
  where day ='#'
) s
left join
unionM_tmp_muid u on (s.lon = u.lon and s.lat = u.lat)
"
insert_daily_sql="
select device,lat,lon,time_start,time_end,
       type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10,
       province, type, city, area, street, plat
from daily_tmp_muid t
cluster by device
"
insert_dinein_sql="
select tmp.device,
       case
         when tmp.name is null or tmp.name='' or tmp.name='null' then '未知'
         else tmp.name
       end as name,
       case
         when mapping.taste is null or mapping.taste='' or mapping.taste='null' then '未知'
         else mapping.taste
       end as taste,
       case
         when mapping.type1 is null or mapping.type1='' or mapping.type1='null' then '未知'
         else mapping.type1
       end as type1,
       case
         when mapping.type2 is null or mapping.type2='' or mapping.type2='null' then '未知'
         else mapping.type2
       end as type2,
       tmp.time as time,
       case
         when mapping.price_level is null or mapping.price_level in ('', 'null', 'NULL') then -1
         else mapping.price_level
       end as price_level
from
(
  select device,name,time
  from
  (
    select t.device, if(regexp_replace(t.type_4.name,'\\\(.*?\\\)','')='',t.type_4.name,regexp_replace(t.type_4.name,'\\\(.*?\\\)','')) as name,
           case
             when (time_start >= '05:00:00' and time_start < '10:00:00') then '早餐'
             when (time_start >= '10:00:00' and time_start < '11:00:00') then '早午餐'
             when (time_start >= '11:00:00' and time_start < '14:00:00') then '午餐'
             when (time_start >= '14:00:00' and time_start < '17:00:00') then '下午茶'
             when (time_start >= '17:00:00' and time_start < '20:00:00') then '晚餐'
             else '夜宵'
           end as time
    from daily_tmp_muid t
  ) dd
  group by dd.device,dd.name,time
  cluster by dd.device
) tmp
left join
$dim_catering_cate_mapping mapping on tmp.name = mapping.name
"
add_poi="
insert overwrite table $udf_tmp
select u.geohash_center5,u.geohash_center6,u.geohash_center7,u.geohash_center8,
       e.lat,e.lon,u.type_1,u.type_2,u.type_3,u.type_4,u.type_5,u.type_6,u.type_7,u.type_8,u.type_9,u.type_10
from except_tmp_muid e
left join
udf_tmp_muid u on (e.lat = u.lat
             and e.lon=u.lon)
"
exceptsql="
select c.alat as lat,c.alon as lon
from
(
  select a.lat as alat,a.lon as alon,b.lat as blat ,b.lon as blon
  from
  (
    select lat,lon
    from sdk_lbs_tmp_muid
    group by lat,lon
  ) a
  left join
  (
    select lat,lon
    from lbs_poi_tmp_muid
  ) b on (a.lat = b.lat
         and a.lon = b.lon)
) c
where (c.blat is null
and c.blon is null)
"
/opt/cloudera/parcels/CDH/bin/spark-submit --master yarn-cluster \
--class com.youzu.mob.label.LbsPOITmpMuid \
--driver-memory 18G \
--executor-memory 18G \
--executor-cores 4 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=30 \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.dynamicAllocation.schedulerBacklogTimeout=5s \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=3000 \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.4 \
--conf spark.yarn.executor.memoryOverhead=10240 \
--conf spark.driver.maxResultSize=4g \
--driver-java-options "-XX:MaxPermSize=1g" \
/home/dba/mobdi_center/lib/MobDI-spark-1.0-SNAPSHOT-jar-with-dependencies.jar "${day}" "${beforeNum}" "${lbs_tmp}" "${poi_tmp}" "${exceptsql}" "${tmp_sql}" "${add_poi}" $partitions "${insert_daily_sql}" "${insert_dinein_sql}" $isHbase "$filesnum" "$dropsql" "$dws_device_catering_dinein_di" "$dws_device_lbs_poi_10type_di" "$udf_tmp" "$dw_base_poi_l1_geohash"

# 合并小文件
hive -e"
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $udf_tmp
select * from $udf_tmp
"
  dataNum=`hive -e"select * from $udf_tmp limit 10;"|wc -l`
  if [ $dataNum -gt 0 ]; then
  hive -e"
    set io.sort.mb=50;
    set hive.groupby.skewindata=true;
    set hive.auto.convert.join=true;
    set hive.map.aggr=true;
    set dfs.socket.timeout=3600000;
    set dfs.datanode.socket.write.timeout=3600000;
    set mapred.task.timeout=3600000;
    set mapreduce.job.reduce.slowstart.completedmaps=1;
    set hive.merge.mapfiles = true;
    set hive.merge.mapredfiles = true;
    set hive.merge.size.per.task = 130023424;
    set hive.merge.smallfiles.avgsize=16000000;

    insert overwrite table $dim_lat_lon_poi_mapping
    select geohash_center5,geohash_center6,geohash_center7,geohash_center8,
           lat,lon,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10
    from
    (
      select geohash_center5,geohash_center6,geohash_center7,geohash_center8,
             lat,lon,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10
      from $udf_tmp

      union  all

      select geohash_center5,geohash_center6,geohash_center7,geohash_center8,
             lat,lon,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10
      from $dim_lat_lon_poi_mapping
    ) ranks
"

fi

end_time=`date +%s`
cost_time=$((end_time - b_time))
echo "This script runtime is $cost_time s"