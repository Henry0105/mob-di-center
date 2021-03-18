#!/bin/sh

set -e -x

: '
@owner:hushk,luost
@describe:android_id增量表更新
@projectName:mobdi
@BusinessName:id_mapping
'

:<<!
@parameters
@insert_day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

insert_day=$1

#input
device_oiid_incr=dm_mobdi_mapping.device_oiid_mapping_sec_di
pid_mapping_incr=dm_mobdi_topic.dws_pid_mapping_sec_di

#mapping
dim_isid_attribute_full_par_sec=dm_mobdi_mapping.dim_isid_attribute_full_par_secview

#output
android_id_mapping_incr=dm_mobdi_topic.dws_id_mapping_android_sec_di

hive -e "
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5500m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5500m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5500m' -XX:+UseG1GC;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function ARRAY_DISTINCT as 'com.youzu.mob.java.udf.ArrayDistinct';

with android_device_carrierarray as (
    select t1.device,
           collect_set(isid_attribute.operator) as carrierarray
    from
    (
        select device,
               isid1
        from $android_id_mapping_incr
        lateral view explode(ARRAY_DISTINCT(split(isid, ','), isidarray)) t_isid as isid1
        where day = '$insert_day'
        and size(ARRAY_DISTINCT(split(isid, ','), isidarray)) > 0
    ) t1
    inner join
    (
        select isid,
               operator
        from $dim_isid_attribute_full_par_sec
    ) isid_attribute
    on t1.isid1 = isid_attribute.isid
    group by t1.device
),

android_device_pid as (
    select coalesce(t1.device,t2.device) as device,
           concat_ws(',',t2.pidno,t2.ext_pidno,t2.sms_pidno,t2.mobauth_pid) as pid,
           concat_ws(',',t2.pidno_tm,t2.ext_pidno_tm,t2.sms_pidno_tm,t2.mobauth_pid_tm) as pid_tm,
           t2.isid,
           t2.isid_tm
    from
    (
        select case when device is not null or length(trim(device)) > 0 then device else concat('',rand()) end as device
        from $android_id_mapping_incr
        where day = '$insert_day'
    ) t1
    full join
    (
        select device,
               pidno,
               pidno_tm,
               ext_pidno,
               ext_pidno_tm,
               sms_pidno,
               sms_pidno_tm,
               concat_ws(',',isid,ext_isid) as isid,
               concat_ws(',',isid_tm,ext_isid_tm) as isid_tm,
               mobauth_pid,
               mobauth_pid_tm
        from $pid_mapping_incr
        where day= '$insert_day'
        and plat = 1
    )t2
    on t1.device = t2.device
),

android_device_oiid as (
    select device,
           concat_ws(',', collect_list(oiid)) as oiid,
           concat_ws(',', collect_list(oiid_tm)) as oiid_tm
    from
    (
      select device,
             oiid,
             max(oiid_tm) as oiid_tm
      from $device_oiid_incr
      where day = '$insert_day'
      and device is not null
      group by device,oiid
    )a
    group by device
)

insert overwrite table $android_id_mapping_incr partition(day='$insert_day')
select coalesce(e.device,f.device) as device,
       mcid,
       mcidarray,
       ieid,
       ieidarray,
       snid,
       asid,
       adrid,
       ssnid,
       pidno,
       pidno_tm,
       isid,
       isid_tm,
       isidarray,
       snsuid_list,
       ssnid_tm,
       snid_tm,
       mcid_tm,
       ieid_tm,
       asid_tm,
       adrid_tm,
       carrierarray,
       pid,
       pid_tm,
       orig_ieid,
       orig_ieid_tm,
       f.oiid,
       f.oiid_tm
from
(
    select coalesce(c.device,d.device) as device,
           mcid,
           mcidarray,
           ieid,
           ieidarray,
           snid,
           asid,
           adrid,
           ssnid,
           pidno,
           pidno_tm,
           d.isid,
           d.isid_tm,
           isidarray,
           snsuid_list,
           ssnid_tm,
           snid_tm,
           mcid_tm,
           ieid_tm,
           asid_tm,
           adrid_tm,
           carrierarray,
           d.pid,
           d.pid_tm,
           orig_ieid,
           orig_ieid_tm
    from
    (
        select a.device,
               mcid,
               mcidarray,
               ieid,
               ieidarray,
               snid,
               asid,
               adrid,
               ssnid,
               pidno,
               pidno_tm,
               isidarray,
               snsuid_list,
               ssnid_tm,
               snid_tm,
               mcid_tm,
               ieid_tm,
               asid_tm,
               adrid_tm,
               case when b.carrierarray is null or size(b.carrierarray) = 0 then null else b.carrierarray end as carrierarray,
               orig_ieid,
               orig_ieid_tm
        from
        (
            select *
            from $android_id_mapping_incr
            where day = '$insert_day'
        ) a
        left join android_device_carrierarray b
        on ((case when a.device is not null or length(trim(a.device)) > 0 then a.device else concat('', rand()) end) = b.device)
    )c
    full join android_device_pid d
    on ((case when c.device is not null or length(trim(c.device)) > 0 then c.device else concat('', rand()) end) = d.device)
)e
full join android_device_oiid f
on ((case when e.device is not null or length(trim(e.device)) > 0 then e.device else concat('', rand()) end) = f.device);
"
