#!/bin/bash
: '
@owner: haom
@describe:
@projectName:
@BusinessName:
@SourceTable:dm_mobdi_master.dwd_device_location_di
@TargetTable:dm_mobdi_topic.dws_device_location_staying_di
@TableRelation:dm_mobdi_master.dwd_device_location_di->dm_mobdi_topic.dws_device_location_staying_di
'

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

insert_day=$1

# check source data: #######################
CHECK_DATA()
{
  local src_path=$1
  hadoop fs -test -e $src_path
  if [[ $? -eq 0 ]] ; then
    # path存在
    src_data_du=`hadoop fs -du -s $src_path | awk '{print $1}'`
    # 文件夹大小不为0
    if [[ $src_data_du != 0 ]] ;then
      return 0
    else
      return 1
    fi
  else
      return 1
  fi
}

#后面有时间把改成循环判断，wait 30分钟
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='pv'"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='auto_location_info'"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='base_station_info'"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='location_info'"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='log_run_new'"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='log_wifi_info'"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table='t_location'"


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
  muid, duid, lat, lon, time, processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy,apppkg,abnormal_flag,ga_abnormal_flag
  from
  (
    select
    muid, duid, lat, lon, time, processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy,
    row_number() over(partition by muid, lat, lon, time order by level) as rank,apppkg,abnormal_flag,ga_abnormal_flag
    from (
      select muid, duid,
      round(cast(lat as double), 6) as lat,
      round(cast(lon as double), 6) as lon,
      time, processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy,
      case when type = 'gps' then 1
           when type = 'bssid' then 2
           when type = 'base' then 4
           when type = 'ip' then 5
           else 999 end as level,apppkg,abnormal_flag,
           ga_abnormal_flag
      from $dwd_device_location_di
      where day='$insert_day'
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
  muid, lat, lon, start_time,
  row_number() over(partition by muid order by start_time,lat,lon) as join_key
  from
    (
    select
    muid, split(split(list1, '=')[0], ',')[0] as lat, split(split(list1, '=')[0], ',')[1] as lon,  split(list1, '=')[1] as start_time
    from
    (
      select
      muid, array_distinct_by_sorted_list(collect_list(concat(latlon, '=', time)), '=') as lists
      from
      (
        select
        muid, time, concat(lat, ',', lon) as latlon
        from ranked_device_time
        distribute by muid sort by muid, time, latlon
      ) ordered_records
      group by muid
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
  device_location_start_time.muid, lat, lon, device_location_start_time.start_time, case when other_start_time.start_time is null then '23:59:59' else other_start_time.start_time end as end_time
  from device_location_start_time
  left join
  (
    select
    muid, start_time, (join_key-1) as join_key_plus  --device_location_start_time的index与 device_location_start_time的下一个index进行匹配
    from device_location_start_time
  ) other_start_time
  on device_location_start_time.muid = other_start_time.muid and device_location_start_time.join_key = other_start_time.join_key_plus
)

insert overwrite table $dws_device_location_staying_di partition (day='$insert_day')
select
device_location_start_and_end_time.muid as device,
ranked_device_time.duid,
device_location_start_and_end_time.lat,
device_location_start_and_end_time.lon,
device_location_start_and_end_time.start_time,
device_location_start_and_end_time.end_time,
processtime, country, province, city, area, street, plat, network, type, data_source, orig_note1, orig_note2, accuracy, apppkg,abnormal_flag,ga_abnormal_flag
from device_location_start_and_end_time
inner join ranked_device_time
on (
  ranked_device_time.muid = device_location_start_and_end_time.muid
  and ranked_device_time.lat = device_location_start_and_end_time.lat
  and ranked_device_time.lon = device_location_start_and_end_time.lon
  and device_location_start_and_end_time.start_time = ranked_device_time.time
)
distribute by device_location_start_and_end_time.muid sort by device_location_start_and_end_time.muid, device_location_start_and_end_time.start_time
;
"
