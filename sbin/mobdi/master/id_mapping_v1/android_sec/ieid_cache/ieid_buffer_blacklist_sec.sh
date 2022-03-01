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
log_device_info_pre_sec_di=dm_mobdi_tmp.log_device_info_pre_sec_di
android_id_mapping_1to2_sec_di=dm_mobdi_tmp.android_id_mapping_1to2_sec_di

# mid
tmp_ieid_huancun_sec_di=dm_mobdi_tmp.tmp_ieid_huancun_sec_di
ieid_huancun_clean_sec=dm_mobdi_tmp.ieid_huancun_clean_sec
dm_ieid_huancun_clean_sec_df=dm_mobdi_tmp.dm_ieid_huancun_clean_sec_df
ieid_huancun_test_clean_group_sec=dm_mobdi_tmp.ieid_huancun_test_clean_group_sec
android_id_mapping_new_ieid_sec=dm_mobdi_tmp.android_id_mapping_new_ieid_sec
ieid_huancun_clean_group_sample_sec=dm_mobdi_tmp.ieid_huancun_clean_group_sample_sec
ieid_huancun_clean_group_sample_model2_sec=dm_mobdi_tmp.ieid_huancun_clean_group_sample_model2_sec
ieid_cnt2_device_modelinfo_format_sec=dm_mobdi_tmp.ieid_cnt2_device_modelinfo_format_sec

# output
dim_ieid_buffer_blacklist_sec=dim_mobdi_mapping.dim_ieid_buffer_blacklist_sec


## 作为当天新增的缓冲数据 device-ieid
HADOOP_USER_NAME=dba hive -v -e"
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
source /home/dba/mobdi/mobdi_sec/master/id_mapping_v2_muid/android_sec/ieid_cache/f_model_clean.sh
HADOOP_USER_NAME=dba hive -v -e "
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


insert overwrite table $dim_ieid_buffer_blacklist_sec partition(day='$day')
select device_help as device ,ieid
from $ieid_cnt2_device_modelinfo_format_sec
where (factory <> factory_help or model <> model_help)
and !((factory = factory_help and factory <> ''
      and ((model = '' and model_help <> '') or (model <> '' and model_help = '')))
      or (model = model_help and model <> '' and ((factory = '' and factory_help <> '') or (factory <> '' and factory_help = ''))))
and (cast(min_serdatetime_help as bigint) - min_serdatetime) > 60;
"