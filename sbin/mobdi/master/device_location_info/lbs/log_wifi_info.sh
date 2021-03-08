#!/bin/bash

set -x -e

if [ -z "$1" ]; then
  exit 1
fi

source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

insert_day=$1
source_table="log_wifi_info"
plus_1day=`date +%Y%m%d -d "${insert_day} +1 day"`
plus_2day=`date +%Y%m%d -d "${insert_day} +2 day"`
echo "startday: "$insert_day
echo "endday:   "$plus_2day

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
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_log_wifi_info_sec_di/day=${insert_day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_log_wifi_info_sec_di/day=${plus_1day}"
CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_log_wifi_info_sec_di/day=${plus_2day}"

CHECK_DATA "hdfs://ShareSdkHadoop/user/hive/warehouse/dm_mobdi_master.db/dwd_device_location_di/day=${insert_day}/source_table=${source_table}"
# ##########################################

###源表
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
#dwd_device_location_di=dm_mobdi_master.dwd_device_location_di

###目标表
device_location_daily_incr=dm_mobdi_tmp.device_location_daily_incr

hive -v -e "
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=10;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;

insert overwrite table $device_location_daily_incr partition (day='$insert_day', source_table='log_wifi_info')
select t1.muid as device,duid,lat,lon,time,processtime,country,province,city,area,street,plat,network,type,data_source,
orig_note1,orig_note2,accuracy,apppkg,orig_note3,ga_abnormal_flag
from
(select
  trim(lower(nvl(muid, ''))) as muid
from $dwd_log_wifi_info_sec_di
where day between '$plus_1day' and '$plus_2day'  --取前两天的数据中时间是前面第三天的数据
and from_unixtime(CAST(datetime/1000 as BIGINT), 'yyyyMMdd') = '$insert_day' --取clienttime转换为当日的数据
and trim(lower(muid)) rlike '^[a-f0-9]{40}$' and trim(muid)!='0000000000000000000000000000000000000000'
and plat in (1,2)
group by muid) t1
left join
(select * from $dwd_device_location_di
where day='$insert_day'
and source_table='log_wifi_info') t2
on t1.muid = t2.muid
;
"
