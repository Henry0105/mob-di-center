#!/bin/bash
: '
@owner: menff
@describe: 设备的社交账号信息汇总
20190313 update:
    1.表迁移:dw_mobdi_md.sdk_device_snsuid_list_android和dw_mobdi_md.sdk_device_snsuid_list_ios
    2.如下表废弃:
      dm_sdk_master.android_id_mapping
      dm_sdk_master.android_id_mapping_full
      dm_sdk_master.ios_id_mapping
      dm_sdk_master.ios_id_mapping_full
      dm_sdk_master.android_id_full
      dm_sdk_master.ios_id_full
'

set -x -e
export LANG=zh_CN.UTF-8
cd `dirname $0`
libPath=/home/dba/lib
: '
@parameters
@DATE:传入日期参数，执行该日期前一个月的数据
'
currentDay=$1
prevMonth=`date -d "$currentDay 1 month ago" +%Y%m`
firstDay=${prevMonth}"01"
nextMonth=`date -d "${firstDay} 1 month" "+%Y%m%d"`
lastDay=`date -d "${nextMonth} -1 days" "+%Y%m%d"`
prev18Month=`date -d "$currentDay 18 month ago" +%Y%m`

# input
dwd_log_share_new_di=dm_mobdi_master.dwd_log_share_new_di
dwd_log_oauth_new_di=dm_mobdi_master.dwd_log_oauth_new_di

# md
tmp_device_snsuid_android=dw_mobdi_tmp.tmp_device_snsuid_android    # 只用到一次    dw_mobdi_tmp.tmp_device_snsuid_android
tmp_device_snsuid_ios=dw_mobdi_tmp.tmp_device_snsuid_ios            # 只用到一次    dw_mobdi_tmp.tmp_device_snsuid_ios
dws_device_snsuid_list_android=dm_mobdi_topic.dws_device_snsuid_list_android    # 放入topic   dm_mobdi_topic.dws_device_snsuid_list_android
dws_device_snsuid_list_ios=dm_mobdi_topic.dws_device_snsuid_list_ios            # 放入topic   dm_mobdi_topic.dws_device_snsuid_list_ios

# output
dws_device_snsuid_mi=dm_mobdi_topic.dws_device_snsuid_mi      # 只用到一次    dm_mobdi_topic.dws_device_snsuid_mi
dws_device_snsuid_mf=dm_mobdi_topic.dws_device_snsuid_mf      # 只用到一次    dm_mobdi_topic.dws_device_snsuid_mf




hive -v -e "
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;



insert overwrite table $dws_device_snsuid_mi partition (par_time='$prevMonth')
select snsplat, lower(trim(snsuid)), lower(trim(deviceid)), min(day) as minday,max(day) as maxday, plat
from
(
  select muid as deviceid, plat, snsplat, snsuid, day
  from $dwd_log_share_new_di
  where day>=$firstDay
  and day<=$lastDay
  and snsplat is not null
  and length(trim(snsuid))>0
  and length(trim(muid))>0

  union all

  select muid as deviceid, plat, snsplat, snsuid, day
  from $dwd_log_oauth_new_di
  where day>=$firstDay and day<=$lastDay
  and snsplat is not null
  and length(trim(snsuid))>0
  and length(trim(muid))>0
)s
group by snsplat, lower(trim(snsuid)),lower(trim(deviceid)), plat;



insert overwrite table $tmp_device_snsuid_android
select lower(trim(deviceid)), snsplat, lower(trim(snsuid)), max(maxday) maxday
from $dws_device_snsuid_mi
where par_time>=$prev18Month
and par_time<=$prevMonth
and plat=1
group by lower(trim(deviceid)), snsplat, lower(trim(snsuid));



insert overwrite table $tmp_device_snsuid_ios
select lower(trim(deviceid)), snsplat, lower(trim(snsuid)), max(maxday) maxday
from rp_mobdi_app.rp_sdk_device_snsuid_mi
where par_time>=$prev18Month
and par_time<=$prevMonth
and plat=2
group by lower(trim(deviceid)), snsplat, lower(trim(snsuid));
"

# spark sql生成snsuid_list
sql="
insert overwrite table $dws_device_snsuid_list_android
select deviceid, merge(snsplat,snsuid,maxday) as snsuid_list
from $tmp_device_snsuid_android
group by deviceid
"

spark2-submit --master yarn \
--deploy-mode cluster \
--class com.youzu.mob.sns.GlobalMapping \
--name "GlobalMapping_snsuid_list_android" \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.sql.shuffle.partitions=130 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.dynamicAllocation.minExecutors=10 \
--executor-memory 15g \
--executor-cores 3 \
--driver-java-options "-XX:MaxPermSize=1g" \
$libPath/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$sql"

sql="
insert overwrite table $dws_device_snsuid_list_ios
select deviceid, merge(snsplat,snsuid,maxday) as snsuid_list
from $tmp_device_snsuid_ios
group by deviceid
"

spark2-submit --master yarn \
--deploy-mode cluster \
--class com.youzu.mob.sns.GlobalMapping \
--name "GlobalMapping_snsuid_list_ios" \
--conf spark.hadoop.validateOutputSpecs=false \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.sql.shuffle.partitions=130 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.dynamicAllocation.minExecutors=10 \
--executor-memory 12g \
--executor-cores 3 \
--driver-java-options "-XX:MaxPermSize=1g" \
$libPath/MobDI-spark2-1.0-SNAPSHOT-jar-with-dependencies.jar "$sql"

:<<!
全量数据给ga使用
第一次跑，插入全量数据
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table rp_mobdi_app.rp_device_snsuid_full partition(day='20200101',plat=1)
select snsplat,lower(trim(snsuid)) as snsuid,lower(trim(deviceid)) as deviceid,min(minday) as minday,max(maxday) as maxday
from rp_mobdi_app.rp_sdk_device_snsuid_mi
where par_time<='202001'
and plat=1
group by snsplat,lower(trim(snsuid)),lower(trim(deviceid));

insert overwrite table rp_mobdi_app.rp_device_snsuid_full partition(day='20200101',plat=2)
select snsplat,lower(trim(snsuid)) as snsuid,lower(trim(deviceid)) as deviceid,min(minday) as minday,max(maxday) as maxday
from rp_mobdi_app.rp_sdk_device_snsuid_mi
where par_time<='202001'
and plat=2
group by snsplat,lower(trim(snsuid)),lower(trim(deviceid));
!

fullLastPar=`date -d "$firstDay 1 month ago" +%Y%m%d`
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;


insert overwrite table $dws_device_snsuid_mf partition(day='$firstDay',plat=1)
select snsplat,snsuid,deviceid,min(minday) as minday,max(maxday) as maxday
from
(
  select snsplat,snsuid,deviceid,minday,maxday
  from $dws_device_snsuid_mi
  where par_time='$prevMonth'
  and plat=1

  union all

  select snsplat,snsuid,deviceid,minday,maxday
  from $dws_device_snsuid_mf
  where day='$fullLastPar'
  and plat=1
) t1
group by snsplat,snsuid,deviceid;



insert overwrite table $dws_device_snsuid_mf partition(day='$firstDay',plat=2)
select snsplat,snsuid,deviceid,min(minday) as minday,max(maxday) as maxday
from
(
  select snsplat,snsuid,deviceid,minday,maxday
  from $dws_device_snsuid_mi
  where par_time='$prevMonth'
  and plat=2

  union all

  select snsplat,snsuid,deviceid,minday,maxday
  from $dws_device_snsuid_mf
  where day='$fullLastPar'
  and plat=2
) t1
group by snsplat,snsuid,deviceid;
"
