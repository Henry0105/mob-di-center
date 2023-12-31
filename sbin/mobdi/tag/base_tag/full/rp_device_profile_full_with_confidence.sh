#!/bin/bash
set -x -e

: '
@owner:luost
@describe:device用户全量标签画像
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

day=$1
lastday=`date -d "$day -1 day" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive-env.sh

tmpdb="dw_mobdi_tmp"


##input
device_permanent_place=$label_l2_permanent_place_mf
device_location_3monthly_struct=$rp_device_location_3monthly_struct
device_mapping_label=$label_mapping_type_all_di
device_model_label=$label_model_type_all_di
device_statics_label=$label_statics_type_all_di
device_mintime_full=$device_mintime_mapping
label_grouplist2_di=$label_l1_grouplist2_di
device_cate_preference_incr=$tmpdb.device_cate_preference_incr
#dim_mapping_area_par=dim_sdk_mapping.dim_mapping_area_par

##output
device_profile_label_full=$rp_device_profile_full_with_confidence

monthly_lastpar=`hive -S -e "show partitions $device_permanent_place" |tail -n 1 `
location_monthly_lastpar=`hive -S -e "show partitions $device_location_3monthly_struct" |tail -n 1 `
area_mapping_lastpar=`hive -S -e "show partitions $dim_mapping_area_par" | tail -n 1`

#定义方法
struct_2_str() {
  field="$1"
  as_field="$2"
  
  echo "
  case
  when $field is null then 'unknown'
  else concat_ws(',',
    concat_ws(':', 'lat', $field.lat),
    concat_ws(':', 'lon', $field.lon),
    concat_ws(':', 'province', $field.province),
    concat_ws(':', 'city', $field.city),
    concat_ws(':', 'area', $field.area),
    concat_ws(':', 'street', $field.street),
    concat_ws(':', 'cnt', cast($field.cnt as string))
  )
  end
  as $as_field
  "
}

#实现同步更新设备信息标签的方法，保证同类设备信息同步更新
#目前保证：cell_factory sysver model model_level screensize public_date price 这些标签字段同步更新
#更新逻辑是如果其中有一个字段的值不为null,那么就启动更新
#传入五个参数：
#<tb_name> <new_filed> <old_filed> <as_field> <field_type>:<表别名> <新表字段名> <老表字段名> <表别名> <默认值类型，0为unknown,1为默认值-1>
device_info_update_sync(){
  tb_name="$1"
  new_filed="$2"
  old_filed="$3"
  as_field="$4"
  field_type=$5
  
  base_sql="case 
    when ${tb_name}.cell_factory is not null or ${tb_name}.sysver is not null or ${tb_name}.model is not null
    or (${tb_name}.model_level is not null and ${tb_name}.model_level>-1) or ${tb_name}.screensize is not null 
    or ${tb_name}.public_date is not null or ${tb_name}.price is not null "

  if [ $field_type -eq 0 ];then
  echo "
  ${base_sql}
  then coalesce($new_filed,'unknown')
  else coalesce($old_filed,'unknown')
  end as $as_field
  "
  elif [ $field_type -eq 1 ];then
  echo "
  ${base_sql}
  then coalesce($new_filed,-1)
  else coalesce($old_filed,-1)
  end as $as_field
  "
  fi
}

#定义国家省份城市等地域信息同步更新的方法,同步更新：国家省份城市字段
#传入五个参数：
#<tb_name> <new_filed> <old_filed> <as_field> <field_type>:<表别名> <新表字段名> <老表字段名> <字段别名> <默认值类型，0为unknown,1为未知 2为默认值-1>
device_area_update_sync(){

  tb_name="$1"
  new_filed="$2"
  old_filed="$3"
  as_field="$4"
  field_type=$5

  base_sql="case 
  when ${tb_name}.country is not null or ${tb_name}.province is not null or ${tb_name}.city is not null 
  or ${tb_name}.country_cn is not null or ${tb_name}.province_cn is not null or ${tb_name}.city_cn is not null"
  
  if [ $field_type -eq 0 ];then
  echo "
    ${base_sql}
  then coalesce($new_filed,'unknown')
  else coalesce($old_filed,'unknown')
  end as $as_field
   "
   elif [ $field_type -eq 1 ];then
     echo "
    ${base_sql}
    then coalesce($new_filed,'未知')
    else coalesce($old_filed,'未知')
    end as $as_field
   "
   elif [ $field_type -eq 2 ];then
    echo "
    ${base_sql}
    then coalesce($new_filed,-1)
    else coalesce($old_filed,-1)
    end as $as_field
   "
   fi
}

#step 1:device_mapping_info_label,incr && full
start=`date +%Y-%m-%d:%H:%M:%S`

echo `date +%Y-%m-%d:%H:%M:%S` "start"

newVer=${day}.1000
lastVer=${lastday}.1000

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;

DROP temporary FUNCTION IF EXISTS cnCodeConvert;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function concat2 as 'com.youzu.mob.java.udaf.concatfortwofields';

insert overwrite table $device_profile_label_full partition(version='$newVer')
select un.device,
       coalesce(mapping.carrier,full.carrier,'unknown') as carrier,
       coalesce(mapping.network,full.network,'unknown') as network,
       `device_info_update_sync 'mapping' 'mapping.cell_factory' 'full.cell_factory' 'cell_factory' 0`,
       `device_info_update_sync 'mapping' 'mapping.sysver' 'full.sysver' 'sysver' 0`,
       `device_info_update_sync 'mapping' 'mapping.model' 'full.model' 'model' 0`,
       `device_info_update_sync 'mapping' 'mapping.model_level' 'full.model_level' 'model_level' 1`,
       `device_info_update_sync 'mapping' 'mapping.screensize' 'full.screensize' 'screensize' 0 `,
       `device_area_update_sync 'mapping' 'mapping.country' 'full.country' 'country' 0`,
       `device_area_update_sync 'mapping' 'mapping.province' 'full.province' 'province' 0`,
       `device_area_update_sync 'mapping' 'mapping.city' 'full.city' 'city' 0`,
       `device_area_update_sync 'mapping' 'mapping.city_level' 'full.city_level' 'city_level' 2 `,
       `device_area_update_sync  'mapping' 'mapping.country_cn' 'full.country_cn' 'country_cn' 1`,
       `device_area_update_sync 'mapping' 'mapping.province_cn' 'full.province_cn' 'province_cn' 1`,
       `device_area_update_sync 'mapping' 'mapping.city_cn' 'full.city_cn' 'city_cn' 1 `,
       coalesce(lower(mapping.breaked),lower(full.breaked),'unknown') as breaked,
       `device_area_update_sync 'mapping' 'mapping.city_level_1001' 'full.city_level_1001' 'city_level_1001' 2`,
       `device_info_update_sync 'mapping' 'mapping.public_date' 'full.public_date' 'public_date' 0`,
       coalesce(mapping.identity,full.identity,'-1') as identity,
       coalesce(models.gender,full.gender,-1) as gender,
       coalesce(models.gender_cl,full.gender_cl,-1) as gender_cl,
       coalesce(models.agebin,full.agebin,-1) as agebin,
       coalesce(models.agebin_cl,full.agebin_cl,-1) as agebin_cl,
       coalesce(models.car,full.car,-1) as car,
       coalesce(models.car_cl,full.car_cl,-1) as car_cl,
       coalesce(models.married,full.married,-1) as married,
       coalesce(models.married_cl,full.married_cl,-1) as married_cl,
       coalesce(models.edu,full.edu,-1) as edu,
       coalesce(models.edu_cl,full.edu_cl,-1) as edu_cl,
       coalesce(models.income,full.income,-1) as income,
       coalesce(models.income_cl,full.income_cl,-1) as income_cl,
       coalesce(models.house,full.house,-1) as house,
       coalesce(models.house_cl,full.house_cl,-1) as house_cl,
       coalesce(models.kids,full.kids,-1) as kids,
       coalesce(models.kids_cl,full.kids_cl,-1) as kids_cl,
       coalesce(models.occupation,full.occupation,-1) as occupation,
       coalesce(models.occupation_cl,full.occupation_cl,-1) as occupation_cl,
       coalesce(models.industry,full.industry,-1) as industry,
       coalesce(models.industry_cl,full.industry_cl,-1) as industry_cl,
       coalesce(models.life_stage,full.life_stage,'unknown') as life_stage,
       coalesce(models.special_time,full.special_time,'unknown') as special_time,
       coalesce(models.consum_level,full.consum_level,-1) as consum_level,
       coalesce(models.agebin_1001,full.agebin_1001,-1) as agebin_1001,
       coalesce(models.agebin_1001_cl,full.agebin_1001_cl,-1) as agebin_1001_cl,
       coalesce(models.tag_list,full.tag_list,'unknown') as tag_list,
       coalesce(models.repayment,full.repayment,-1) as repayment,
       coalesce(models.segment,full.segment,-1) as segment,
       coalesce(stats.applist,full.applist,'unknown') as applist,
       coalesce(stats.tot_install_apps,full.tot_install_apps,-1) as tot_install_apps,
       coalesce(stats.nationality,full.nationality,'unknown') as nationality,
       coalesce(stats.nationality_cn,full.nationality_cn,'未知')as nationality_cn,
       coalesce(mapping.day,models.day,stats.day,full.last_active) as last_active,
       case
         when (stats.group_list is null or stats.group_list in ('unknown',''))
              and (full.group_list is null or full.group_list in ('unknown',''))
              and coalesce(models.car,full.car, -1) > 0
              then '28'
         when (stats.group_list is null or stats.group_list in ('unknown',''))
              and full.group_list is not null
              and full.group_list not in ('unknown','')
              and not array_contains(split(full.group_list,','),'28')
              and coalesce(models.car,full.car, -1) > 0
              then concat_ws(',',full.group_list,'28')
         when (stats.group_list is null or stats.group_list in ('unknown',''))
              and full.group_list is not null
              and array_contains(split(full.group_list,','),'28')
              and coalesce(models.car,full.car, -1) > 0
              then full.group_list
         when stats.group_list is not null
              and stats.group_list not in ('unknown','')
              and not array_contains(split(stats.group_list,','),'28')
              and coalesce(models.car,full.car, -1) > 0
              then concat_ws(',',stats.group_list,'28')
         when (stats.group_list is null or stats.group_list in ('unknown',''))
              and (full.group_list is null or full.group_list in ('unknown','','28'))
              and coalesce(models.car,full.car, -1) < 1
              then 'unknown'
         when (stats.group_list is null or stats.group_list in ('unknown',''))
              and full.group_list is not null
              and full.group_list not in ('unknown','','28')
              and array_contains(split(full.group_list,','),'28')
              and coalesce(models.car,full.car, -1) < 1
              then regexp_replace(full.group_list, ',28|28,', '')
         when (stats.group_list is null or stats.group_list in ('unknown',''))
              and full.group_list is not null
              and full.group_list not in ('unknown','','28')
              and not array_contains(split(full.group_list,','),'28')
              and coalesce(models.car,full.car, -1) < 1
              then full.group_list
         else stats.group_list
       end as group_list,
       coalesce(full.first_active_time,mintime.day,'$day') as first_active_time,
       coalesce(stats.catelist,full.catelist,'unknown') as catelist,
       coalesce(preference.cate_preference_list,full.cate_preference_list,'') as cate_preference_list,
       coalesce(d.country,full.permanent_country,'')as permanent_country,
       coalesce(d.province,full.permanent_province,'') as permanent_province,
       coalesce(d.city,full.permanent_city,'') as permanent_city,
       coalesce(d.permanent_country_cn,full.permanent_country_cn,'') as permanent_country_cn,
       coalesce(d.permanent_province_cn,full.permanent_province_cn,'') as permanent_province_cn,
       coalesce(d.permanent_city_cn,full.permanent_city_cn,'') as permanent_city_cn,
       coalesce(location.workplace,full.workplace,'unknown') as workplace,
       coalesce(location.residence,full.residence,'unknown') as residence,
       coalesce(mapping.mapping_flag,full.mapping_flag,0) as mapping_flag ,
       coalesce(models.model_flag,full.model_flag,0) as model_flag,
       coalesce(stats.stats_flag,full.stats_flag,0) as stats_flag ,
       coalesce(stats.processtime,full.processtime,'unknown')as processtime,
       coalesce(mapping.day,
                models.day,
                stats.day,
                case when d.day < full.processtime_all then null else d.day end,
                case when location.day < full.processtime_all then null else location.day end,
                full.processtime_all) as processtime_all,
       `device_info_update_sync 'mapping' 'mapping.price' 'full.price' 'price' 0`,
       coalesce(d.permanent_city_level,full.permanent_city_level,-1) as permanent_city_level,
       coalesce(models.income_1001,full.income_1001,-1) as income_1001,
       coalesce(models.income_1001_cl,full.income_1001_cl,-1) as income_1001_cl,
       coalesce(models.occupation_1001,full.occupation_1001,-1) as occupation_1001,
       coalesce(models.occupation_1001_cl,full.occupation_1001_cl,-1) as occupation_1001_cl,
       coalesce(models.consume_level,full.consume_level,-1) as consume_level,
       coalesce(models.consume_level_cl,full.consume_level_cl,-1) as consume_level_cl,
       coalesce(models.consume_1001,full.consume_1001,-1) as consume_1001,
       coalesce(grouplist2.grouplist,full.group_list2,'unknown') as group_list2,
       coalesce(mapping.day,
                models.day,
                stats.day,
                case when d.day < full.processtime_all then null else d.day end,
                case when location.day < full.processtime_all then null else location.day end,
                full.processtime_all) as update_time,
       coalesce(models.agebin_1002,full.agebin_1002,-1) as agebin_1002,
       coalesce(models.agebin_1002_cl,full.agebin_1002_cl,-1) as agebin_1002_cl,
       coalesce(models.agebin_1003,full.agebin_1003,-1) as agebin_1003,
       coalesce(models.agebin_1003_cl,full.agebin_1003_cl,-1) as agebin_1003_cl,
       `device_info_update_sync 'mapping' 'mapping.factory_cn' 'full.factory_cn' 'factory_cn' 0`,
       `device_info_update_sync 'mapping' 'mapping.factory_clean_subcompany' 'full.factory_clean_subcompany' 'factory_clean_subcompany' 0`,
       `device_info_update_sync 'mapping' 'mapping.factory_cn_subcompany' 'full.factory_cn_subcompany' 'factory_cn_subcompany' 0`,
       `device_info_update_sync 'mapping' 'mapping.sim_type' 'full.sim_type' 'sim_type' 0`,
       `device_info_update_sync 'mapping' 'mapping.screen_size' 'full.screen_size' 'screen_size' 0`,
       `device_info_update_sync 'mapping' 'mapping.cpu' 'full.cpu' 'cpu' 0`
from
(
  select device
  from
  (
    select device
    from $device_mapping_label
    where day='$day'
    and device rlike '[a-f0-9]{40}'
    and device!='0000000000000000000000000000000000000000'

    union all

    select device
    from $device_model_label
    where day='$day'
    and device rlike '[a-f0-9]{40}'
    and device!='0000000000000000000000000000000000000000'

    union all

    select device
    from $device_statics_label
    where day='$day'
    and device rlike '[a-f0-9]{40}'
    and device!='0000000000000000000000000000000000000000'

    union all

    select device
    from $device_profile_label_full
    where version = '$lastVer'
  ) base
  group by device
)un
left join
(
  select *
  from $device_profile_label_full
  where version = '$lastVer'
) full on un.device=full.device
left join
(
  select *,1 as mapping_flag
  from $device_mapping_label
  where day='$day'
) mapping on un.device=mapping.device
left join
(
  select *,1 as model_flag
  from $device_model_label
  where day='$day'
) models on un.device=models.device
left join
(
  select *,1 as stats_flag
  from $device_statics_label
  where day='$day'
) stats on un.device=stats.device
left join
(
  select device,country,province,city,permanent_city_level,permanent_country_cn,permanent_province_cn,permanent_city_cn,day
  from $device_permanent_place
  where ${monthly_lastpar}
)d on un.device=d.device
left join
(
  select device,workplace,residence,day
  from $device_location_3monthly_struct
  where ${location_monthly_lastpar}
)location on un.device=location.device
left join
(
  select device,day
  from $device_mintime_full
  where plat=1
) mintime on un.device=mintime.device
left join
(
  select device,cate_preference_list
  from dw_mobdi_md.device_cate_preference_list
) preference on un.device=preference.device
left join
(
  select device,grouplist
  from $label_grouplist2_di
  where day='$day'
) grouplist2 on un.device=grouplist2.device;
"

#实现删除过期的分区的功能，只保留最近7个分区,20200809.1000分区为最初修复产生的分区，这里给保存下来
for old_version in `hive -e "show partitions $device_profile_label_full " | grep -v '20200809.1000' | grep -v '202101150' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table $device_profile_label_full drop if exists partition($old_version)"
done

#建立视图，之前的全量full表（rp_mobdi_app.rp_device_profile_full_history_true）和其视图下线
hive -v -e "
create or replace view rp_mobdi_report.rp_device_profile_full_with_confidence_view as
select *
from $device_profile_label_full
where version='${newVer}';
"