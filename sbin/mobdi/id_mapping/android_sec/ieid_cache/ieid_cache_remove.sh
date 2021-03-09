#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1
p1day=`date -d "$day -1 days" +%Y%m%d`
p3day=`date -d "$day -3 days" +%Y%m%d`

# input
dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di
android_id_mapping_1to1_sec_di=dw_mobdi_md.android_id_mapping_1to1_sec_di
android_id_mapping_sec_df=dm_mobdi_mapping.android_id_mapping_sec_df

# mid
log_device_info_pre_sec_di=dw_mobdi_md.log_device_info_pre_sec_di
tmp_ieid_huancun_sec_di=dw_mobdi_md.tmp_ieid_huancun_sec_di
android_id_mapping_1to2_sec_di=dw_mobdi_md.android_id_mapping_1to2_sec_di
ieid_huancun_clean_sec=dw_mobdi_md.ieid_huancun_clean_sec
dm_ieid_huancun_clean_sec_df=dw_mobdi_md.dm_ieid_huancun_clean_sec_df
ieid_huancun_test_clean_group_sec=dw_mobdi_md.ieid_huancun_test_clean_group_sec
android_id_mapping_new_ieid_sec=dw_mobdi_md.android_id_mapping_new_ieid_sec
ieid_huancun_clean_group_sample_sec=dw_mobdi_md.ieid_huancun_clean_group_sample_sec
ieid_huancun_clean_group_sample_model2_sec=dw_mobdi_md.ieid_huancun_clean_group_sample_model2_sec
ieid_cnt2_device_modelinfo_format_sec=dw_mobdi_md.ieid_cnt2_device_modelinfo_format_sec
ieid_device_every_day_total_sec=dw_mobdi_md.ieid_device_every_day_total_sec

# mapping
sysver_mapping_par=dm_sdk_mapping.sysver_mapping_par

# output
ieid_buffer_blacklist_sec=dm_mobdi_mapping.ieid_buffer_blacklist_sec


full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'android_id_mapping_sec_df', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)


## 新增数据清洗
hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function arrayDistinct as 'com.youzu.mob.java.udf.ArrayDistinct';
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5200m';
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

insert overwrite table $log_device_info_pre_sec_di partition(day='$day')
select device, factory, model, ieid, serdatetime, sysver
from
(
  select device, factory, model, ieid, serdatetime,
  case
    when lower(trim(info.sysver)) = 'null' or info.sysver is null or trim(info.sysver) = '' or trim(info.sysver) = '未知' then 'unknown'
    when info.sysver = sysver_mapping.vernum then substr(sysver_mapping.sysver, 1, 3)
    when substring(regexp_replace(info.sysver, 'Android|[.]|[ ]', ''), 1, 2) in ('11','15','16','20','21','22','23','24','30','31','32','40','41','42','43','44','50','51','60','70','71','80','81','90') then concat(substring(regexp_replace(info.sysver, 'Android|[.]|[ ]', ''), 1, 1), '.', substring(regexp_replace(info.sysver, 'Android|[.]|[ ]', ''), 2, 1))
  else 'unknown'
  end as sysver
  from
  (
    select device, serdatetime,
    case
      when (length(trim(ieid)) <= 0 or ieid is null) and (length(trim(ieid_arr)) <= 0 or ieid_arr is null) then null
      when (length(trim(ieid)) <= 0 or ieid is null) and length(trim(ieid_arr)) > 0 then trim(ieid_arr)
      when length(trim(ieid)) > 0 and (length(trim(ieid_arr)) <= 0 or ieid_arr is null)then trim(ieid)
    else concat(trim(ieid), ',', trim(ieid_arr))
    end as ieid, factory, model, sysver, day
    from
    (
      select device, unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss') as serdatetime,
      if(length(trim(ieid))=0, null, ieid) as ieid,
      nvl(ieidarray, array()) as ieidarray,
      if(length(concat_ws(',',arrayDistinct(ieidarray)))>30,concat_ws(',',arrayDistinct(ieidarray)) ,null) as ieid_arr,
      factory, model, sysver, day
      from $dwd_log_device_info_jh_sec_di as jh
      where jh.day ='$day' and jh.plat = 1 and device is not null and length(device) = 40 and device = regexp_extract(device, '([a-f0-9]{40})', 0) and breaked = false
     ) as m
  ) as info
  left join
  (
    select *
    from $sysver_mapping_par
    where version = '1002'
  ) as sysver_mapping
  on info.sysver = sysver_mapping.vernum
) as a
where device is not null and length(device) = 40 and device = regexp_extract(device, '([a-f0-9]{40})', 0) and ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other')) or (ieid is not null and trim(ieid) not in ('', ',')))
group by device, factory, model, ieid, serdatetime, sysver;
"

## 作为当天新增的缓冲数据 device-ieid
hive -v -e"
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5200m';
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

insert overwrite table $android_id_mapping_1to2_sec_di partition(day='$day')
select device,ieid
from(
     select device,ieid,count(1) over (partition by ieid) as d_cnt
        from(
        select device,ieid
        from (
            select device, ieid from  $android_id_mapping_1to1_sec_di where day='$p1day'
            union all
            select device,ieid_split as ieid from $log_device_info_pre_sec_di lateral view explode(split(ieid,',')) t as ieid_split
             where day = '$day' and ieid is not null and length(trim(ieid))>0
        )t1
        group by device,ieid
     ) t2
 ) t3
where d_cnt=2;


insert overwrite table $tmp_ieid_huancun_sec_di partition(day='$day')
select device, factory, model, ieid, serdatetime, sysver
from
(
  select device, factory, model, mytable.ieid_split as ieid, serdatetime, sysver
  from
  (
    select device, factory, model, ieid, serdatetime, sysver
    from $log_device_info_pre_sec_di
    where ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0'))) and (ieid is not null and ieid <> '')
    and day='$day'
  ) as m
  lateral view explode(split(ieid, ',')) mytable as ieid_split  --这一步会过滤掉ieid为空或null的


  union all

  select device, factory, model, ieid, serdatetime, sysver
  from $log_device_info_pre_sec_di
  where ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0'))) and (ieid is null or ieid = '')
  and day='$day'
) as a
group by device, factory, model, ieid, serdatetime, sysver;

alter table $tmp_ieid_huancun_sec_di  drop partition(day='$p3day');
"


## 厂商 机型清洗
source /home/dba/mobdi/mobdi_sec/master/id_mapping_v1/ieid_cache/f_model_clean.sh
hive -v -e "
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5200m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

drop table if exists $ieid_huancun_clean_sec;
create table $ieid_huancun_clean_sec stored as orc as
select device, factory, model, ieid, serdatetime, sysver
from
(
  select device,
  `factory_model_clean`,
  case
    when ieid is null or trim(ieid) in ('', '000000000000000', '00000000000000') then ''
  else trim(ieid)
  end as ieid, serdatetime, sysver
  from $tmp_ieid_huancun_sec_di
  where day='$day'
) as a
where (factory <> '' or model <> '') and ieid <> ''
group by device, factory, model, ieid, serdatetime, sysver;

insert overwrite table $dm_ieid_huancun_clean_sec_df partition(day='$day')
select device,factory,model,ieid,serdatetime,sysver
    from(
     select device,factory,model,ieid,serdatetime,sysver
     from
     $dm_ieid_huancun_clean_sec_df where day='$p1day'
     union all
     select
     device,factory,model,ieid,serdatetime,sysver
     from
     $ieid_huancun_clean_sec
) t
group by device,factory,model,ieid,serdatetime,sysver;

alter table $dm_ieid_huancun_clean_sec_df drop partition(day='$p3day');


drop table if exists $ieid_huancun_test_clean_group_sec;
create table $ieid_huancun_test_clean_group_sec stored as orc as
select device, ieid, factory, model, sysver, min_serdatetime, count(*) over(partition by device, ieid) as model_cnt
from
(
  select device, ieid, factory, model, sysver, serdatetime as min_serdatetime
  from
  (
    select device, ieid, factory, model, sysver, serdatetime, row_number() over(partition by device, ieid, factory, model order by serdatetime asc) as num
    from $dm_ieid_huancun_clean_sec_df where day='$day'
  ) as m
  where num = 1
) as a ;


drop table if exists $android_id_mapping_new_ieid_sec;
create table $android_id_mapping_new_ieid_sec stored as orc as
select t1.device,t1.ieid
from
    (select device,ieid from $android_id_mapping_1to2_sec_di where day='$day') t1
    inner join
    (select ieid from $ieid_huancun_clean_sec) t2
    on t1.ieid = t2.ieid
group by t1.ieid,t1.device;


drop table if exists $ieid_huancun_clean_group_sample_sec;
create table $ieid_huancun_clean_group_sample_sec as
select a.device, a.ieid, b.factory, b.model, b.min_serdatetime, from_unixtime(b.min_serdatetime, 'yyyy-MM-dd HH:mm:ss') as min_serdatetime_format, b.model_cnt, b.sysver
from $android_id_mapping_new_ieid_sec as a
left join $ieid_huancun_test_clean_group_sec as b
on a.device = b.device and a.ieid = b.ieid;


drop table if exists $ieid_huancun_clean_group_sample_model2_sec;
create table $ieid_huancun_clean_group_sample_model2_sec as
select ieid, device, factory, model, min_serdatetime, min_serdatetime_format, sysver, split(help, '=xyz=')[0] as device_help, split(help, '=xyz=')[1] as factory_help, split(help, '=xyz=')[2] as model_help, split(help, '=xyz=')[3] as min_serdatetime_help, split(help, '=xyz=')[4] as min_serdatetime_format_help, split(help, '=xyz=')[5] as sysver_help
from
(
  select device, ieid, factory, model, min_serdatetime, min_serdatetime_format, sysver, lead(concat(device, '=xyz=', factory, '=xyz=', model, '=xyz=', min_serdatetime, '=xyz=', min_serdatetime_format, '=xyz=', sysver, '=xyz='), 1) over(partition by device, ieid order by min_serdatetime asc, device asc) as help
  from $ieid_huancun_clean_group_sample_sec
  where model_cnt = 2
) as a
where help is not null;


drop table if exists $ieid_cnt2_device_modelinfo_format_sec;
create table $ieid_cnt2_device_modelinfo_format_sec as
select ieid, device, factory, model, min_serdatetime, min_serdatetime_format, sysver, split(help, '=xyz=')[0] as device_help, split(help, '=xyz=')[1] as factory_help, split(help, '=xyz=')[2] as model_help, split(help, '=xyz=')[3] as min_serdatetime_help, split(help, '=xyz=')[4] as min_serdatetime_format_help, split(help, '=xyz=')[5] as sysver_help
from
(
  select device, ieid, factory, model, min_serdatetime, min_serdatetime_format, sysver, lead(concat(device, '=xyz=', factory, '=xyz=', model, '=xyz=', min_serdatetime, '=xyz=', min_serdatetime_format, '=xyz=', sysver, '=xyz='), 1) over(partition by ieid order by min_serdatetime asc, device asc) as help
  from
  (
    select device, ieid, factory, model, min_serdatetime, min_serdatetime_format, sysver
    from
    (
      select device, ieid, factory, model, min_serdatetime, min_serdatetime_format, sysver
      from $ieid_huancun_clean_group_sample_sec
      where model_cnt = 1

      union all

      select device, ieid,
      case
        when factory <> '' then factory
        when model = model_help and model <> '' and factory = '' and factory_help <> '' then factory_help
      else factory
      end as factory,
      case
        when model <> '' then model
        when factory = factory_help and factory <> '' and model = '' and model_help <> '' then model_help
      else model
      end as model, min_serdatetime, min_serdatetime_format, sysver
      from $ieid_huancun_clean_group_sample_model2_sec
    ) as m
    group by device, ieid, factory, model, min_serdatetime, min_serdatetime_format, sysver
  ) as n
) as a
where help is not null;


insert overwrite table $ieid_buffer_blacklist_sec partition(day='$day')
select device_help as device ,ieid
from $ieid_cnt2_device_modelinfo_format_sec
where (factory <> factory_help or model <> model_help)
and !((factory = factory_help and factory <> ''
      and ((model = '' and model_help <> '') or (model <> '' and model_help = '')))
      or (model = model_help and model <> '' and ((factory = '' and factory_help <> '') or (factory <> '' and factory_help = ''))))
and (cast(min_serdatetime_help as bigint) - min_serdatetime) > 60;
"

hive -v -e"
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function cache_remove as 'com.youzu.mob.java.udf.CacheRemove';

SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=false;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
SET mapreduce.map.memory.mb=8192;
set mapreduce.map.java.opts='-Xmx5g';
set mapreduce.child.map.java.opts='-Xmx6g';
set mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts='-Xmx6g';

drop table if exists $ieid_device_every_day_total_sec;
create table $ieid_device_every_day_total_sec stored as orc as
select device,concat_ws(',',collect_set(ieid)) as ieids
from
(select device,ieid from  $ieid_buffer_blacklist_sec where day='$day' group by device,ieid) t1
group by device;


insert overwrite table $android_id_mapping_sec_df partition(version='$day.1001')
select
t1.device,mcid,mcid_tm,mcid_ltm,mcidarray,mcidarray_tm,
nvl(if(t2.ieids is not null,split(cache_remove(t1.ieid,ieid_tm,ieid_ltm,t2.ieids),'_')[0],ieid),'') as ieid,
nvl(if(t2.ieids is not null,split(cache_remove(t1.ieid,ieid_tm,ieid_ltm,t2.ieids),'_')[1],ieid_tm),'') as ieid_tm,
nvl(if(t2.ieids is not null,split(cache_remove(t1.ieid,ieid_tm,ieid_ltm,t2.ieids),'_')[2],ieid_ltm),'') as ieid_ltm,

ieidarray,ieidarray_tm,snid,snid_tm,snid_ltm,asid,asid_tm,asid_ltm,
adrid,adrid_tm,adrid_ltm,ssnid,ssnid_tm,ssnid_ltm,isid,isid_tm,isid_ltm,isidarray,isidarray_tm,pid,pid_tm,pid_ltm,
snsuid_list,snsuid_list_tm,carrierarray,

nvl(if(t2.ieids is not null,split(cache_remove(t1.orig_ieid,orig_ieid_tm,orig_ieid_ltm,t2.ieids),'_')[0],orig_ieid),'') as orig_ieid,
nvl(if(t2.ieids is not null,split(cache_remove(t1.orig_ieid,orig_ieid_tm,orig_ieid_ltm,t2.ieids),'_')[1],orig_ieid_tm),'') as orig_ieid_tm,
nvl(if(t2.ieids is not null,split(cache_remove(t1.orig_ieid,orig_ieid_tm,orig_ieid_ltm,t2.ieids),'_')[2],orig_ieid_ltm),'') as orig_ieid_ltm,
oiid,oiid_tm,oiid_ltm
from
(select * from $android_id_mapping_sec_df where version='$day.1000') t1
left join
$ieid_device_every_day_total_sec t2
on t1.device=t2.device;
"

# 创建/替换视图
hive -e "
create or replace view dm_mobdi_mapping.android_id_mapping_full_sec_view
as
select * from $android_id_mapping_sec_df
where version='${day}.1001'
"