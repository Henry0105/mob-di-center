#!/bin/bash
: '
@owner: haom
@describe:t-3 的 staying_daily
@projectName:
@BusinessName:
@SourceTable:dm_mobdi_master.dwd_device_location_info_di
@TargetTable:dm_mobdi_topic.dws_device_location_staying_di
@TableRelation:dm_mobdi_master.dwd_device_location_info_di->dm_mobdi_topic.dws_device_location_staying_di
'

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#input
#dwd_device_location_info_di=dm_mobdi_master.dwd_device_location_info_di
#out
#dws_device_location_staying_di=dm_mobdi_topic.dws_device_location_staying_di
device_location_info_db=${dwd_device_location_info_di%.*}
device_location_info_tb=${dwd_device_location_info_di#*.}

day=$1
insert_day=`date -d "$day +2 days" +%Y-%m-%d`

# check source data: ####################### 新流程建表后可删，只保留一个staying_daily即可
while true
do
  hadoop fs -test -e "hdfs://ShareSdkHadoop/user/hive/warehouse/$device_location_info_db.db/$device_location_info_tb/day=${day}"
  if [[ $? -eq 0 ]] ; then
    # path存在
    src_data_date=`hadoop fs -ls "hdfs://ShareSdkHadoop/user/hive/warehouse/$device_location_info_db.db/$device_location_info_tb/day=${day}" | awk '{print $6}'`
    # 判断几个分区的文件更新时间是否是在3天后
    if [[ ${src_data_date:0:11} > $insert_day && ${src_data_date:11:11} > $insert_day && ${src_data_date:22:11} > $insert_day && ${src_data_date:33:11} > $insert_day && ${src_data_date:44:11} > $insert_day && ${src_data_date:55:11} > $insert_day && ${src_data_date:66:11} > $insert_day && ${src_data_date:77:11} > $insert_day ]] ;then
      break
    else
      sleep 1800
      echo "===========  wait for dwd_device_location_info_di ============="
    fi
  fi
done


hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function array_distinct_by_sorted_list as 'com.youzu.mob.java.udf.ArrayDistinctBySortedList';

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.optimize.skewjoin = true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3072m';
SET mapreduce.child.map.java.opts='-Xmx3072m';
SET hive.exec.reducers.bytes.per.reducer = 1000000000;
set mapreduce.reduce.memory.mb=6144;

with ranked_device_time as (
  select
  device, duid, lat, lon, time, processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg,abnormal_flag,ga_abnormal_flag
  from
  (
    select
    device, duid, lat, lon, time, processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy,
    row_number() over(partition by device, lat, lon, time order by level) as rank,apppkg,abnormal_flag,ga_abnormal_flag
    from (
      select device, duid,
      round(cast(lat as double), 6) as lat,
      round(cast(lon as double), 6) as lon,
      time, processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy,
      case when type = 'gps' then 1
           when type = 'bssid' then 2
           when type = 'base' then 4
           when type = 'ip' then 5
           else 999 end as level,apppkg,abnormal_flag,
           ga_abnormal_flag
      from $dwd_device_location_info_di
      where day='$day'
      and (type <> 'ip' or data_source in('pv', 'run')) and data_source<>'wifi_scan_list'
    ) location_daily
  ) ranked_by_device_time
  where rank = 1
),

--计算起始时间并排序打上行号
--首选根据时间排序，再将已排序处于相同位置的记录取第一条的时间，最后根据时间顺序打上行号
--array_distinct_by_sorted_list功能是基于已排序的列表去重，去重原则是取每个分组的第一条数据。
device_location_start_time as (
  select
  device, lat, lon, start_time,
  row_number() over(partition by device order by start_time,lat,lon) as join_key
  from
    (
    select
    device, split(split(list1, '=')[0], ',')[0] as lat, split(split(list1, '=')[0], ',')[1] as lon,  split(list1, '=')[1] as start_time
    from
    (
      select
      device, array_distinct_by_sorted_list(collect_list(concat(latlon, '=', time)), '=') as lists
      from
      (
        select
        device, time, concat(lat, ',', lon) as latlon
        from ranked_device_time
        distribute by device sort by device, time, latlon
      ) ordered_records
      group by device
    ) distincted_records
    lateral view explode(lists) t_list as list1
  ) ranked_records
),

--使用起始时间表和其自身进行join得到下一行的起始时间作为结束时间
--如，根据
--  home    07:00:00
--  school  08:00:00
--  home    12:30:01
--  school  14:00:00
--处理为：
--  home   (1) 07:00:00  (2-1)  08:00:00
--  school (2) 08:00:00  (3-1)  12:30:01
--  home   (3) 12:30:01  (4-1)  14:00:00
--  school (4) 14:00:00  (5-1)  23:59:59

device_location_start_and_end_time as (
  select
  device_location_start_time.device, lat, lon, device_location_start_time.start_time, case when other_start_time.start_time is null then '23:59:59' else other_start_time.start_time end as end_time
  from device_location_start_time
  left join
  (
    select
    device, start_time, (join_key-1) as join_key_plus  --device_location_start_time的index与 device_location_start_time的下一个index进行匹配
    from device_location_start_time
  ) other_start_time
  on device_location_start_time.device = other_start_time.device and device_location_start_time.join_key = other_start_time.join_key_plus
)

insert overwrite table $dws_device_location_staying_di partition (day='$day')
select
device_location_start_and_end_time.device,
ranked_device_time.duid,
device_location_start_and_end_time.lat,
device_location_start_and_end_time.lon,
device_location_start_and_end_time.start_time,
device_location_start_and_end_time.end_time,
processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy, apppkg,abnormal_flag,ga_abnormal_flag
from device_location_start_and_end_time
inner join ranked_device_time
on (
  ranked_device_time.device = device_location_start_and_end_time.device
  and ranked_device_time.lat = device_location_start_and_end_time.lat
  and ranked_device_time.lon = device_location_start_and_end_time.lon
  and device_location_start_and_end_time.start_time = ranked_device_time.time
)
distribute by device_location_start_and_end_time.device sort by device_location_start_and_end_time.device, device_location_start_and_end_time.start_time
;
"
