#!/bin/sh

set -e -x

: '
@owner:hushk,luost
@describe:android_id_mapping每日增量表
@projectName:mobdi
@BusinessName:id_mapping
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期
!

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

#input
log_device_info_jh=dm_mobdi_master.dwd_log_device_info_jh_sec_di

#mapping
sdk_device_snsuid_list_android=dm_mobdi_topic.dws_device_snsuid_list_android

#output
android_id_mapping_incr=dm_mobdi_topic.dws_id_mapping_android_sec_di


#置空函数，简化代码
empty2null() {
f="$1"
echo "if(length($f) <= 0, null, $f)"
}

HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_mcid as 'com.youzu.mob.java.udf.GetMcidByWlan0';
create temporary function combine_unique as 'com.youzu.mob.java.udf.CombineUniqueUDAF';
create temporary function ieid_array_union as 'com.youzu.mob.java.udf.IeidArrayUnion';

set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5200m' -XX:+UseG1GC;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.reuse.jvm.num.tasks=10;
set mapred.tasktracker.map.tasks.maximum=24;
set mapred.tasktracker.reduce.tasks.maximum=24;


insert overwrite table $android_id_mapping_incr partition(day = '$day')
select b.device as device,
       case when length(mcid) = 0 then null else mcid end as mcid,
       case when size(mcidarray[0]) = 0 then null else mcidarray end as mcidarray,
       case when length(ieid) = 0 then null else ieid end as ieid,
       case when size(ieidarray) = 0 then null else ieidarray end as ieidarray,
       case when length(snid) = 0 then null else snid end as snid,
       case when length(asid) = 0 then null else asid end as asid,
       case when length(adrid) = 0 then null else adrid end as adrid,
       case when length(ssnid) = 0 then null else ssnid end as ssnid,
       case when length(pid) = 0 then null else pid end as pidno,
       case when length(pid_tm) = 0 then null else pid_tm end as pidno_tm,
       case when length(isid) = 0 then null else isid end as isid,
       case when length(isid_tm) = 0 then null else isid_tm end as isid_tm,
       case when size(isidarray) = 0 then null else isidarray end as isidarray,
       c.snsuid_list as snsuid_list,
       ssnid_tm,
       snid_tm,
       mcid_tm,
       ieid_tm,
       asid_tm,
       adrid_tm,
       array() as carrierarray,
       null as pid,
       null as  pid_tm,
       case when length(orig_ieid) = 0 then null else orig_ieid end as orig_ieid,
       orig_ieid_tm,
       null as oiid,
       null as oiid_tm
from
(
    select device,
           concat_ws(',', collect_list(mcid)) as mcid,
           sort_array(collect_set(mcidmap)) as mcidarray,
           coalesce(collect_list(ieid)[0],'') as orig_ieid,
           concat_ws(',', if(size(collect_list(ieid))>0, collect_list(ieid),null), if(size(collect_list(ieid_arr))>0, collect_list(ieid_arr), null)) as ieid,
           combine_unique(ieidarray) as ieidarray,
           concat_ws(',', collect_list(snid)) as snid,
           concat_ws(',', collect_list(snid_tm)) as snid_tm,
           concat_ws(',', collect_list(asid)) as asid,
           concat_ws(',', collect_list(adrid)) as adrid,
           concat_ws(',', collect_list(ssnid)) as ssnid,
           concat_ws(',', collect_list(pid)) as pid,
           concat_ws(',', collect_list(isid)) as isid,
           concat_ws(',', collect_list(isid_tm)) as isid_tm,
           combine_unique(isidarray) as isidarray,
           concat_ws(',', collect_list(mcid_tm)) as mcid_tm,
           concat_ws(',', if(size(collect_list(ieid_tm))>0, collect_list(ieid_tm)[0],null)) as orig_ieid_tm,
           concat_ws(',', if(size(collect_list(ieid_tm))>0, collect_list(ieid_tm),null), if(size(collect_list(ieid_arr_tm))>0, collect_list(ieid_arr_tm), null)) as ieid_tm,
           concat_ws(',', collect_list(asid_tm)) as asid_tm,
           concat_ws(',', collect_list(adrid_tm)) as adrid_tm,
           concat_ws(',', collect_list(ssnid_tm)) as ssnid_tm,
           concat_ws(',', collect_list(pid_tm)) as pid_tm
    from
    (
        select device,

               case
                 when get_mcid(mcidarray) is not null and get_mcid(mcidarray) <> '02:00:00:00:00:00'
                   then lower(get_mcid(mcidarray))
                 else `empty2null 'mcid'`
               end as mcid,

               m as mcidmap,
               if(length(trim(ieid)) = 0,null,trim(ieid)) as ieid,
               if(ieidarray is not null and size(ieidarray) > 0, ieidarray, null) as ieidarray,

               case
                 when length(ieid_array_union('field',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                   then ieid_array_union('field',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                 else null
               end as ieid_arr,

               `empty2null 'snid'` as snid,
               `empty2null 'asid'` as asid,
               `empty2null 'adrid'` as adrid,
               `empty2null 'ssnid'` as ssnid,
               `empty2null 'pid'` as pid,
               `empty2null 'isid'` as isid,
               cast(if(length(trim(isid)) = 0 or isid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as isid_tm,

               if(isidarray is not null and size(isidarray) > 0,isidarray, null) as isidarray,

               case
                 when get_mcid(mcidarray) is not null and get_mcid(mcidarray) <> '02:00:00:00:00:00'
                   then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
                 when `empty2null 'mcid'` is not null
                   then cast(unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss') as string)
                 else null
               end as mcid_tm,

               cast(if(length(trim(ieid)) = 0 or ieid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as ieid_tm,

               case
                 when length(ieid_array_union('field',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')))>0
                   then ieid_array_union('date',ieidarray,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss'))
                 else null
               end as ieid_arr_tm,

               cast(if(length(trim(snid)) = 0 or snid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as snid_tm,
               cast(if(length(trim(asid)) = 0 or asid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as asid_tm,
               cast(if(length(trim(adrid)) = 0 or adrid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as adrid_tm,
               cast(if(length(trim(ssnid)) = 0 or ssnid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as ssnid_tm,
               cast(if(length(trim(pid)) = 0 or pid is null,null,unix_timestamp(serdatetime,'yyyy-MM-dd HH:mm:ss')) as string) as pid_tm
        from $log_device_info_jh as jh
        lateral view explode(coalesce(if(size(mcidarray) = 0,null,mcidarray), array(map()))) tf as m
        where jh.day = '$day'
        and jh.plat = 1
        and device is not null
        and length(device)=40
        and device = regexp_extract(device,'([a-f0-9]{40})', 0)
    ) device_info_jh
    group by device
) as b
left join $sdk_device_snsuid_list_android c
on (case when length(b.device)=40 then b.device else concat('',rand()) end = c.deviceid);
"
