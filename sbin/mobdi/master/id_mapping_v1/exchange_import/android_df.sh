#!/bin/sh

set -e -x

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

insert_day=$1

# input
dim_device_merge_df=ex_log.dim_device_merge_df
dim_device_mac_merge_df=ex_log.dim_device_mac_merge_df
dim_device_imei_merge_df=ex_log.dim_device_imei_merge_df
dim_device_serialno_merge_df=ex_log.dim_device_serialno_merge_df
dim_device_imsi_merge_df=ex_log.dim_device_imsi_merge_df
dim_device_phone_merge_df=ex_log.dim_device_phone_merge_df
dim_device_oaid_merge_df=ex_log.dim_device_oaid_merge_df

# output
dim_id_mapping_android_df=ex_log.dim_id_mapping_android_df


HADOOP_USER_NAME=dba hive -e"
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapreduce.map.memory.mb=10240;
set mapreduce.map.java.opts='-Xmx8192m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx8192m';
set mapreduce.reduce.memory.mb=14336;
set mapreduce.reduce.java.opts='-Xmx12288m' -XX:+UseG1GC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;



insert overwrite table $dim_id_mapping_android_df partition (version='${insert_day}.1002')
select
    ff.device as device,
    de_mac.mac as mac,
    de_mac.mac_tm as mac_tm,
    de_mac.mac_ltm as mac_ltm,
    de_imei.imei as imei,
    de_imei.imei_tm as imei_tm,
    de_imei.imei_ltm as imei_ltm,
    de_serialno.serialno as serialno,
    de_serialno.serialno_tm as serialno_tm,
    de_serialno.serialno_ltm as serialno_ltm,
    de_imsi.imsi as imsi,
    de_imsi.imsi_tm as imsi_tm,
    de_imsi.imsi_ltm as imsi_ltm,
    de_phone.phone as phone,
    de_phone.phone_tm as phone_tm,
    de_phone.phone_ltm as phone_ltm,
    de_oaid.oaid as oaid,
    de_oaid.oaid_tm as oaid_tm,
    de_oaid.oaid_ltm as oaid_ltm
from
(select device from $dim_device_merge_df where day='${insert_day}') ff
left join
(
  select device,mac, mac_tm, mac_ltm
  from $dim_device_mac_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and mac is not null and length(mac)>0
) de_mac on ff.device=de_mac.device
left join
(
  select device,imei, imei_tm, imei_ltm
  from $dim_device_imei_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and imei is not null and length(imei)>0
) de_imei on ff.device=de_imei.device
left join
(
  select device, serialno, serialno_tm, serialno_ltm
  from $dim_device_serialno_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and serialno is not null and length(serialno)>0
) de_serialno on ff.device=de_serialno.device
left join
(
  select device, imsi, imsi_tm, imsi_ltm
  from $dim_device_imsi_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and imsi is not null and length(imsi)>0
) de_imsi on ff.device=de_imsi.device
left join
(
  select device, phone, phone_tm, phone_ltm
  from $dim_device_phone_merge_df
  where day='${insert_day}.1002' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and phone is not null and length(phone)>0
) de_phone on ff.device=de_phone.device
left join
(
  select device, oaid, oaid_tm, oaid_ltm
  from $dim_device_oaid_merge_df
  where day='${insert_day}' and device is not null and length(device)= 40 and device = regexp_extract(device,'([a-f0-9]{40})', 0)
  and oaid is not null and length(oaid)>0
) de_oaid on ff.device=de_oaid.device
"
