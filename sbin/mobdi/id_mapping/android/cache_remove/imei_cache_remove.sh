#!/bin/bash

set -x -e

day=$1
p1day=`date -d "$day -1 days" +%Y%m%d`
p3day=`date -d "$day -3 days" +%Y%m%d`


full_partition_sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('dm_mobdi_mapping', 'android_id_mapping_full', 'version');
drop temporary function GET_LAST_PARTITION;
"
full_last_version=(`hive -e "$full_partition_sql"`)

#input
log_device_info_jh=dw_sdk_log.log_device_info_jh
tmp_android_id_mapping_1to1=dm_mobdi_tmp.tmp_android_id_mapping_1to1
dim_id_mapping_android_df=dm_mobdi_mapping.dim_id_mapping_android_df


# mid
tmp_log_device_info_pre=dm_mobdi_tmp.tmp_log_device_info_pre
tmp_imei_huancun=dm_mobdi_tmp.tmp_imei_huancun
tmp_android_id_mapping_1to2=dm_mobdi_tmp.tmp_android_id_mapping_1to2
tmp_imei_huancun_clean=dm_mobdi_tmp.tmp_imei_huancun_clean
tmp_dm_imei_huancun_clean_df=dm_mobdi_tmp.tmp_dm_imei_huancun_clean_df
tmp_imei_huancun_test_clean_group=dm_mobdi_tmp.tmp_imei_huancun_test_clean_group
tmp_android_id_mapping_new_imei=dm_mobdi_tmp.tmp_android_id_mapping_new_imei
tmp_imei_huancun_clean_group_sample=dm_mobdi_tmp.tmp_imei_huancun_clean_group_sample
tmp_imei_huancun_clean_group_sample_model2=dm_mobdi_tmp.tmp_imei_huancun_clean_group_sample_model2
tmp_imei_cnt2_device_modelinfo_format=dm_mobdi_tmp.tmp_imei_cnt2_device_modelinfo_format
tmp_imei_device_every_day_total=dm_mobdi_tmp.tmp_imei_device_every_day_total

#mapping
sysver_mapping_par=dm_sdk_mapping.sysver_mapping_par

#output
dim_imei_buffer_blacklist=dm_mobdi_mapping.dim_imei_buffer_blacklist
dim_id_mapping_android_df_view=dm_mobdi_mapping.dim_id_mapping_android_df_view

## 新增数据清洗
HADOOP_USER_NAME=dba hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function imeiarray_clear as 'com.youzu.mob.java.udf.ImeiArrayClear';
create temporary function imsiarray_clear as 'com.youzu.mob.java.udf.ImsiArrayClear';
create temporary function get_mac as 'com.youzu.mob.java.udf.GetMacByWlan0';
create temporary function combine_unique as 'com.youzu.mob.java.udf.CombineUniqueUDAF';
create temporary function extract_phone_num as 'com.youzu.mob.java.udf.PhoneNumExtract';
create temporary function luhn_checker as 'com.youzu.mob.java.udf.LuhnChecker';
create temporary function array_distinct as 'com.youzu.mob.java.udf.ArrayDistinct';
create temporary function imei_array_union as 'com.youzu.mob.mobdi.ImeiArrayUnion';
create temporary function arrayDistinct as 'com.youzu.mob.java.udf.ArrayDistinct';
set hive.exec.parallel=true;



insert overwrite table $tmp_log_device_info_pre partition(day='$day')
select device, factory, model, imei, serdatetime, sysver
from
(
  select device, factory, model, imei, serdatetime,
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
        when (length(trim(imei)) <= 0 or imei is null) and (length(trim(imei_arr)) <= 0 or imei_arr is null) then null
        when (length(trim(imei)) <= 0 or imei is null) and length(trim(imei_arr)) > 0 then trim(imei_arr)
        when length(trim(imei)) > 0 and (length(trim(imei_arr)) <= 0 or imei_arr is null)then trim(imei)
      else concat(trim(imei), ',', trim(imei_arr))
      end as imei, factory, model, sysver, dt
    from
    (
      select
          muid as device,
          unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss') as serdatetime,
          case
            when trim(imei) rlike '0{14,17}' then ''
            when length(trim(lower(imei))) = 16 and trim(imei) rlike '^[0-9]+$' then if(imei_verify(regexp_replace(trim(lower(substring(imei,1,14))), ' |/|-|imei:', '')), regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),'')
            when length(trim(lower(imei))) = 16 and trim(imei) not rlike '^[0-9]+$' then ''
            when imei_verify(regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')) then regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')
            else ''
          end as imei,
          nvl(imeiarray_clear(imeiarray), array()) as imeiarray,
          if(length(concat_ws(',',arrayDistinct(imeiarray)))>30,concat_ws(',',arrayDistinct(imeiarray)) ,null) as imei_arr,
          factory, model, sysver, dt
      from $log_device_info_jh as jh
      where jh.dt ='$day' and jh.plat = 1 and muid is not null and length(muid) = 40 and muid = regexp_extract(muid, '([a-f0-9]{40})', 0) and breaked = false
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
where device is not null and length(device) = 40 and device = regexp_extract(device, '([a-f0-9]{40})', 0) and ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other')) or (imei is not null and trim(imei) not in ('', ',')))
group by device, factory, model, imei, serdatetime, sysver;
"

## 作为当天新增的缓冲数据 device-imei
HADOOP_USER_NAME=dba hive -v -e"
insert overwrite table $tmp_android_id_mapping_1to2 partition(day='$day')
select device,imei
from(
     select device,imei,count(1) over (partition by imei) as d_cnt
        from(
        select device,imei
        from (
            select device, imei from  $tmp_android_id_mapping_1to1 where day='$p1day'
            union all
            select device,imei_split as imei from $tmp_log_device_info_pre lateral view explode(split(imei,',')) t as imei_split
             where day = '$day' and imei is not null and length(trim(imei))>0
        )t1
        group by device,imei
     ) t2
 ) t3
where d_cnt=2;

insert overwrite table $tmp_imei_huancun partition(day='$day')
select device, factory, model, imei, serdatetime, sysver
from
(
  select device, factory, model, mytable.imei_split as imei, serdatetime, sysver
  from
  (
    select device, factory, model, imei, serdatetime, sysver
    from $tmp_log_device_info_pre
    where ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0'))) and (imei is not null and imei <> '')
    and day='$day'
  ) as m
  lateral view explode(split(imei, ',')) mytable as imei_split  --这一步会过滤掉imei为空或null的


  union all

  select device, factory, model, imei, serdatetime, sysver
  from $tmp_log_device_info_pre
  where ((factory is not null and lower(trim(factory)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0')) or (model is not null and lower(trim(model)) not in ('', 'unknown', 'null', 'none', 'other', '未知', 'na', '0'))) and (imei is null or imei = '')
  and day='$day'
) as a
group by device, factory, model, imei, serdatetime, sysver;

alter table $tmp_imei_huancun  drop partition(day='$p3day');
"

## 厂商 机型清洗
source /home/dba/mobdi/id_mapping/android/cache_remove/cache_remove/f_model_clean.sh
HADOOP_USER_NAME=dba hive -e"
drop table if exists $tmp_imei_huancun_clean;
create table $tmp_imei_huancun_clean stored as orc as
select device, factory, model, imei, serdatetime, sysver
from
(
  select device,
  `factory_model_clean`,
  case
    when imei is null or trim(imei) in ('', '000000000000000', '00000000000000') then ''
  else trim(imei)
  end as imei, serdatetime, sysver
  from $tmp_imei_huancun
  where day='$day'
) as a
where (factory <> '' or model <> '') and imei <> ''
group by device, factory, model, imei, serdatetime, sysver;

insert overwrite table $tmp_dm_imei_huancun_clean_df partition(day='$day')
select device,factory,model,imei,serdatetime,sysver
    from(
     select device,factory,model,imei,serdatetime,sysver
     from
     $tmp_dm_imei_huancun_clean_df where day='$p1day'
     union all
     select
     device,factory,model,imei,serdatetime,sysver
     from
     $tmp_imei_huancun_clean
) t
group by device,factory,model,imei,serdatetime,sysver;

alter table $tmp_dm_imei_huancun_clean_df drop partition(day='$p3day');


drop table if exists $tmp_imei_huancun_test_clean_group;
create table $tmp_imei_huancun_test_clean_group stored as orc as
select device, imei, factory, model, sysver, min_serdatetime, count(*) over(partition by device, imei) as model_cnt
from
(
  select device, imei, factory, model, sysver, serdatetime as min_serdatetime
  from
  (
    select device, imei, factory, model, sysver, serdatetime, row_number() over(partition by device, imei, factory, model order by serdatetime asc) as num
    from $tmp_dm_imei_huancun_clean_df where day='$day'
  ) as m
  where num = 1
) as a ;


drop table if exists $tmp_android_id_mapping_new_imei;
create table $tmp_android_id_mapping_new_imei stored as orc as
select t1.device,t1.imei
from
    (select device,imei from $tmp_android_id_mapping_1to2 where day='$day') t1
    inner join
    (select imei from $tmp_imei_huancun_clean) t2
    on t1.imei = t2.imei
group by t1.imei,t1.device;




drop table if exists $tmp_imei_huancun_clean_group_sample;
create table $tmp_imei_huancun_clean_group_sample as
select a.device, a.imei, b.factory, b.model, b.min_serdatetime, from_unixtime(b.min_serdatetime, 'yyyy-MM-dd HH:mm:ss') as min_serdatetime_format, b.model_cnt, b.sysver
from $tmp_android_id_mapping_new_imei as a
left join $tmp_imei_huancun_test_clean_group as b
on a.device = b.device and a.imei = b.imei;


drop table if exists $tmp_imei_huancun_clean_group_sample_model2;
create table $tmp_imei_huancun_clean_group_sample_model2 as
select imei, device, factory, model, min_serdatetime, min_serdatetime_format, sysver, split(help, '=xyz=')[0] as device_help, split(help, '=xyz=')[1] as factory_help, split(help, '=xyz=')[2] as model_help, split(help, '=xyz=')[3] as min_serdatetime_help, split(help, '=xyz=')[4] as min_serdatetime_format_help, split(help, '=xyz=')[5] as sysver_help
from
(
  select device, imei, factory, model, min_serdatetime, min_serdatetime_format, sysver, lead(concat(device, '=xyz=', factory, '=xyz=', model, '=xyz=', min_serdatetime, '=xyz=', min_serdatetime_format, '=xyz=', sysver, '=xyz='), 1) over(partition by device, imei order by min_serdatetime asc, device asc) as help
  from $tmp_imei_huancun_clean_group_sample
  where model_cnt = 2
) as a
where help is not null;


drop table if exists $tmp_imei_cnt2_device_modelinfo_format;
create table $tmp_imei_cnt2_device_modelinfo_format as
select imei, device, factory, model, min_serdatetime, min_serdatetime_format, sysver, split(help, '=xyz=')[0] as device_help, split(help, '=xyz=')[1] as factory_help, split(help, '=xyz=')[2] as model_help, split(help, '=xyz=')[3] as min_serdatetime_help, split(help, '=xyz=')[4] as min_serdatetime_format_help, split(help, '=xyz=')[5] as sysver_help
from
(
  select device, imei, factory, model, min_serdatetime, min_serdatetime_format, sysver, lead(concat(device, '=xyz=', factory, '=xyz=', model, '=xyz=', min_serdatetime, '=xyz=', min_serdatetime_format, '=xyz=', sysver, '=xyz='), 1) over(partition by imei order by min_serdatetime asc, device asc) as help
  from
  (
    select device, imei, factory, model, min_serdatetime, min_serdatetime_format, sysver
    from
    (
      select device, imei, factory, model, min_serdatetime, min_serdatetime_format, sysver
      from $tmp_imei_huancun_clean_group_sample
      where model_cnt = 1

      union all

      select device, imei,
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
      from $tmp_imei_huancun_clean_group_sample_model2
    ) as m
    group by device, imei, factory, model, min_serdatetime, min_serdatetime_format, sysver
  ) as n
) as a
where help is not null;


insert overwrite table $dim_imei_buffer_blacklist partition(day='$day')
select device_help as device ,imei
from $tmp_imei_cnt2_device_modelinfo_format
where (factory <> factory_help or model <> model_help)
and !((factory = factory_help and factory <> ''
      and ((model = '' and model_help <> '') or (model <> '' and model_help = '')))
      or (model = model_help and model <> '' and ((factory = '' and factory_help <> '') or (factory <> '' and factory_help = ''))))
and (cast(min_serdatetime_help as bigint) - min_serdatetime) > 60;
"

HADOOP_USER_NAME=dba hive -v -e"
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

drop table if exists $tmp_imei_device_every_day_total;
create table $tmp_imei_device_every_day_total stored as orc as
select device,concat_ws(',',collect_set(imei)) as imeis
from
(select device,imei from  $imei_buffer_blacklist where day='$day' group by device,imei) t1
group by device;


insert overwrite table $dim_id_mapping_android_df partition(version='$day.1001')
select
t1.device,mac,mac_tm,mac_ltm,macarray,macarray_tm,
nvl(if(t2.imeis is not null,split(cache_remove(t1.imei,imei_tm,imei_ltm,t2.imeis),'_')[0],imei),'') as imei,
nvl(if(t2.imeis is not null,split(cache_remove(t1.imei,imei_tm,imei_ltm,t2.imeis),'_')[1],imei_tm),'') as imei_tm,
nvl(if(t2.imeis is not null,split(cache_remove(t1.imei,imei_tm,imei_ltm,t2.imeis),'_')[2],imei_ltm),'') as imei_ltm,

imeiarray,imeiarray_tm,serialno,serialno_tm,serialno_ltm,adsid,adsid_tm,adsid_ltm,
androidid,androidid_tm,androidid_ltm,simserialno,simserialno_tm,simserialno_ltm,imsi,imsi_tm,imsi_ltm,imsiarray,imsiarray_tm,phone,phone_tm,phone_ltm,
snsuid_list,snsuid_list_tm,carrierarray,

nvl(if(t2.imeis is not null,split(cache_remove(t1.orig_imei,orig_imei_tm,orig_imei_ltm,t2.imeis),'_')[0],orig_imei),'') as orig_imei,
nvl(if(t2.imeis is not null,split(cache_remove(t1.orig_imei,orig_imei_tm,orig_imei_ltm,t2.imeis),'_')[1],orig_imei_tm),'') as orig_imei_tm,
nvl(if(t2.imeis is not null,split(cache_remove(t1.orig_imei,orig_imei_tm,orig_imei_ltm,t2.imeis),'_')[2],orig_imei_ltm),'') as orig_imei_ltm,
oaid,oaid_tm,oaid_ltm
from
(select * from $dim_id_mapping_android_df where version='$day.1000') t1
left join
$tmp_imei_device_every_day_total t2
on t1.device=t2.device;
"


function qc_id_mapping(){

  cd `dirname $0`
  sh /home/dba/mobdi/qc/real_time_mobdi_qc/qc_id_mapping_view.sh  "$dim_id_mapping_android_df" "${day}.1001" "$dim_id_mapping_android_df_view" || qc_fail_flag=1
  if [[ ${qc_fail_flag} -eq 1 ]]; then
    echo 'qc失败，阻止生成view'
    exit 1
  fi
  echo "qc_id_mapping success!"
}

qc_id_mapping $day



HADOOP_USER_NAME=dba hive -e"
create or replace view $dim_id_mapping_android_df_view
as
select * from $dim_id_mapping_android_df
where version='${day}.1001'
"
