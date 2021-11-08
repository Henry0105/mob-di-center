#!/bin/sh

: '
@owner: wangyc
@describe: 国籍标签
@projectName: MobDI
@BusinessName:profile_model
@SourceTable: rp_mobdi_app.des,dm_mobdi_mapping.device_applist_new,dw_mobdi_md.device_nationality_apprank_uniq,rp_appgo_common.app_rank_monthly_2,dw_mobdi_md.device_nationality_apprank_cnt,dm_sdk_mapping.map_country_sdk,dw_mobdi_md.device_nationality_ip,dw_mobdi_md.device_nationality_base,dm_mobdi_master.device_info_master_full_par,dm_sdk_mapping.app_pkg_mapping_par,dw_mobdi_md.device_nationality_carrier,dw_mobdi_md.device_nationality_permanent_pre,dm_mobdi_master.device_ip_info,dw_mobdi_md.device_nationality_language,dw_mobdi_md.device_nationality_permanent,dm_sdk_mapping.mapping_carrier_country,rp_mobdi_app.device_language
@TargetTable: dw_mobdi_md.device_nationality_apprank_uniq,rp_mobdi_app.device_nationality,dw_mobdi_md.device_nationality_apprank_cnt,dw_mobdi_md.device_nationality_ip,dw_mobdi_md.device_nationality_base,dw_mobdi_md.device_nationality_carrier,dw_mobdi_md.device_nationality_permanent_pre,dw_mobdi_md.device_nationality_language,dw_mobdi_md.device_nationality_permanent
@TableRelation: dm_mobdi_mapping.device_applist_new->dw_mobdi_md.device_nationality_base
                |dw_mobdi_md.device_nationality_base,rp_mobdi_app.device_language->dw_mobdi_md.device_nationality_language
                |dw_mobdi_md.device_nationality_base,dm_mobdi_master.device_ip_info->dw_mobdi_md.device_nationality_ip
                |dw_mobdi_md.device_nationality_base,dm_sdk_mapping.map_country_sdk,rp_mobdi_app.rp_device_location_permanent->dw_mobdi_md.device_nationality_permanent
                |dm_sdk_mapping.mapping_carrier_country,dm_mobdi_master.device_info_master_full_par->dw_mobdi_md.device_nationality_carrier
                |rp_appgo_common.app_rank_monthly_2->dw_mobdi_md.device_nationality_apprank_uniq
                |dw_mobdi_md.device_nationality_apprank_uniq,dm_sdk_mapping.app_pkg_mapping_par,dm_mobdi_mapping.device_applist_new->dw_mobdi_md.device_nationality_apprank_cnt
                |dw_mobdi_md.device_nationality_language,dw_mobdi_md.device_nationality_ip,dw_mobdi_md.device_nationality_permanent,dw_mobdi_md.device_nationality_carrier,dm_sdk_mapping.map_country_sdk,dw_mobdi_md.device_nationality_apprank_cnt->rp_mobdi_app.device_nationality partition


'

set -x -e
export LANG=en_US.UTF-8

: '
@parameters
@indate:输入日期参数
@permanent_last_day:rp_device_location_permanent最新分区
@apprank_last_day:app_rank_monthly_2最新分区
实现步骤: 1.将dm_mobdi_mapping.device_applist_new中的当日设备去重存入dw_mobdi_md.device_nationality_base表
          2.dw_mobdi_md.device_nationality_base与rp_mobdi_app.device_language的最新分区关联得到设备国家数据，
            存入dw_mobdi_md.device_nationality_language
          3.dm_sdk_mapping.map_country_sdk与rp_mobdi_app.rp_device_location_permanent最新分区关联得到设备居住地
            国家数据，结果存入dw_mobdi_md.device_nationality_permanent_pre
          4.dw_mobdi_md.device_nationality_permanent_pre与dw_mobdi_md.device_nationality_base关联，结果存入
            dw_mobdi_md.device_nationality_permanent
          5.rp_appgo_common.app_rank_monthly_2在最新分区以及category_id=2下取app排名的平均值，然后已zone为分组
            依据app排名平均值排序，取前10000条数据，再取app_id唯一的数据，存入dw_mobdi_md.device_nationality_apprank_uniq
          6.从dm_mobdi_master.device_ip_info的当日分区中取出plat=1的所有设备的最新数据，然后与dw_mobdi_md.device_nationality_base
            关联，结果再与dm_sdk_mapping.map_country_sdk关联，得到设备ip国籍中文数据，结果存入dw_mobdi_md.device_nationality_ip
          7.dw_mobdi_md.device_nationality_base与dm_mobdi_master.device_info_master_full_par关联，得到设备运营商
            数据，然后与dm_sdk_mapping.mapping_carrier_country关联得到运营商国籍，结果存入dw_mobdi_md.device_nationality_carrier
          8.dw_mobdi_md.device_nationality_apprank_uniq与dm_sdk_mapping.app_pkg_mapping_par关联，结果去重，再与
            dm_mobdi_mapping.device_applist_new的当日数据关联，结果计数，把设备技术结果最大的数据存入
            dw_mobdi_md.device_nationality_apprank_cnt
          9.dw_mobdi_md.device_nationality_language加上dw_mobdi_md.device_nationality_ip，再加上dw_mobdi_md.device_nationality_permanent
            和dw_mobdi_md.device_nationality_carrier的国籍数据，与dm_sdk_mapping.map_country_sdk关联得到国籍代码
          10.dw_mobdi_md.device_nationality_apprank_cnt与dm_sdk_mapping.map_country_sdk关联，同样得到国际代码
          11.步骤9和步骤10的数据合并去重，同一设备保留计数最多的国籍数据，与dw_mobdi_md.device_nationality_apprank_cnt
             计数大于等于5的数据关联，结果存入rp_mobdi_app.device_nationality
'
start_time=$(date +%s)

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <indate>"
    exit 1
fi

indate=$1


source /home/dba/mobdi_center/conf/hive-env.sh

echo "start step 1"

permanent_sql="show partitions $rp_device_location_permanent;"
permanent_par=(` hive -e "$permanent_sql"`)
permanent_last_par=${permanent_par[(${#permanent_par[*]}-1)]#*=}
permanent_arr=(${permanent_last_par//// })
permanent_last_day=${permanent_arr[0]}

apprank_sql="show partitions $app_rank_monthly_2;"
apprank_par=(` hive -e "$apprank_sql"`)
apprank_last_par=${apprank_par[(${#apprank_par[*]}-1)]#*=}
apprank_arr=(${apprank_last_par//// })
apprank_last_day=${apprank_arr[0]}

carrier_par=` hive -e "show partitions $dwd_device_info_df" | tail -n 1`
carrier_last=${carrier_par%/*}
echo $carrier_last


#input

#device_language=dm_mobdi_report.device_language
#rp_device_location_permanent=dm_mobdi_report.rp_device_location_permanent
#app_rank_monthly_2=rp_appgo_common.app_rank_monthly_2
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df

#mapping
#dim_device_applist_new_di=dim_mobdi_mapping.dim_device_applist_new_di
#dim_map_country_sdk=dim_sdk_mapping.dim_map_country_sdk
#map_country_sdk=dm_sdk_mapping.map_country_sdk
#dim_mapping_carrier_country=dim_sdk_mapping.dim_mapping_carrier_country
#mapping_carrier_country=dm_sdk_mapping.mapping_carrier_country
#dim_app_pkg_mapping_par=dim_sdk_mapping.dim_app_pkg_mapping_par
#app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par


#tmp
tmpdb=$dm_mobdi_tmp
tmp_device_nationality_base=${tmpdb}.device_nationality_base
tmp_device_nationality_language=${tmpdb}.device_nationality_language
tmp_device_nationality_permanent_pre=${tmpdb}.device_nationality_permanent_pre
tmp_device_nationality_permanent=${tmpdb}.device_nationality_permanent
tmp_device_nationality_ip=${tmpdb}.device_nationality_ip
tmp_device_nationality_carrier=${tmpdb}.device_nationality_carrier
tmp_device_nationality_apprank_cnt=${tmpdb}.device_nationality_apprank_cnt

#output
#device_nationality=dm_mobdi_report.device_nationality



#--每天增量更新的base表
hive -e "
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
insert overwrite table $tmp_device_nationality_base
select device
from $dim_device_applist_new_di
where day = ${indate}
and processtime = ${indate}
group by device;
"
#全量device可以从full表获取
lastest_par=` hive -e "show partitions $device_language" | tail -n 1`
echo $lastest_par

wait

echo "start step 2"

#--手机语言
hive -e "
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
insert overwrite table $tmp_device_nationality_language
select a2.device,a2.country
from $tmp_device_nationality_base a1
inner join
(
  select device,country_cn as country
  from $device_language
  where ${lastest_par}
  and country_cn <> ''
)a2 on a1.device = a2.device;
" &

#--常住地 dw_mobdi_md.device_nationality_base,dm_sdk_mapping.map_country_sdk,
#rp_mobdi_app.rp_device_location_permanent->dw_mobdi_md.device_nationality_permanent

#--设备居住地信息
hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $tmp_device_nationality_permanent_pre
select device,ch_name as country
from
(
  select zone,ch_name
  from $dim_map_country_sdk
  group by zone,ch_name
) b2
inner join
(
  select device,
         case
           when province = 'cn_31' then 'hk'
           when province = 'cn_32' then 'mo'
           when province = 'cn_33' then 'tw'
           else country
         end as country
  from $rp_device_location_permanent
  where day = ${permanent_last_day}
  and trim(country) <> ''
) b1 on b1.country = b2.zone;


insert overwrite table $tmp_device_nationality_permanent
select a2.device,a2.country
from $tmp_device_nationality_permanent_pre a2
inner join  $tmp_device_nationality_base a1 on a1.device = a2.device;
" &
wait

echo "start step 3"

#--ip地址
hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
insert overwrite table $tmp_device_nationality_ip
select device,ch_name as country
from
(
  select zone,ch_name
  from $dim_map_country_sdk
  group by zone,ch_name
) b1
inner join
(
  select a2.device,country
  from $tmp_device_nationality_base a1
  inner join
  (
    select device,country
    from
    (
      select device,country,
             row_number() over( partition by device order by flag,timestamp desc) as rn
      from
      (
        select device,
               case
                 when province = 'cn_31' then 'hk'
                 when province = 'cn_32' then 'mo'
                 when province = 'cn_33' then 'tw'
                 else country
               end as country,
               timestamp,
               case
                 when instr(network,'3g')=0 and instr(network,'4g')=0 then 2
                 else 1
               end as flag
        from  $dws_device_ip_info_di
        where day='${indate}'
        and plat=1
        and network is not null
        and network!=''
      )tmp
    )un
    where rn=1
  ) a2 on a1.device = a2.device
) b2 on b1.zone = b2.country;
" &

#--运营商 dm_sdk_mapping.mapping_carrier_country,dw_mobdi_md.device_nationality_hardware->dw_mobdi_md.device_nationality_carrier
hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite table $tmp_device_nationality_carrier
select a2.device,a1.country
from
(
  select operator,country
  from
  (
    select operator,country,
           count(1) over (partition by operator) as cnt
    from
    (
      select operator,country
      from $dim_mapping_carrier_country
      group by operator,country
    )a
  )aa
  where cnt = 1
) a1
inner join
(
  select b1.device,b2.carrier
  from $tmp_device_nationality_base b1
  inner join
  (
    select device,carrier
    from $dwd_device_info_df
    where ${carrier_last}
    and plat = 1
  ) b2 on b1.device = b2.device
) a2 on a1.operator = a2.carrier;
" &
wait
echo "start final step...."

#--安装10个以上上榜APP的设备优先级最高
#--插入正式表
hive -e "
CREATE TABLE IF NOT EXISTS $device_nationality(
device string,
nationality string,
country_code string
)
partitioned by (day string)
stored as orc;
"

# dw_mobdi_md.device_nationality_language,dw_mobdi_md.device_nationality_ip,dw_mobdi_md.device_nationality_permanent,
#dw_mobdi_md.device_nationality_carrier,dm_sdk_mapping.map_country_sdk,dw_mobdi_md.device_nationality_apprank_cnt->rp_mobdi_app.device_nationality partition
hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set hive.exec.parallel=true;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
insert overwrite table $device_nationality partition (day = ${indate})
select device,
       case
         when country_code = 'hk' then '中国'
         when country_code = 'mo' then '中国'
         when country_code = 'tw' then '中国'
         else nationality
       end as nationality,
       case
         when country_code = 'hk' then 'cn'
         when country_code = 'mo' then 'cn'
         when country_code = 'tw' then 'cn'
         else country_code
       end as country_code
from
(
  select b1.device,
         b1.country as nationality,
         lower(b1.country_code) as country_code
  from
  (
    select device,country,country_code
    from
    (
      select device,country,country_code,
             row_number() over (partition by device order by cnt desc,tag) as rank
      from
      (
        select device,country,country_code,tag,cnt
        from
        (
          select device,country,country_code,concat_ws(',',collect_set(tag)) as tag_list,count(1) as cnt
          from
          (
            select a1.device,a2.ch_name as country,upper(a2.zone) as country_code,tag
            from
            (
              select device,country,'2' as tag
              from $tmp_device_nationality_language

              union all

              select device,country,'4' as tag
              from $tmp_device_nationality_ip

              union all

              select device,country,'3' as tag
              from $tmp_device_nationality_permanent

              union all

              select device,country,'1' as tag
              from $tmp_device_nationality_carrier
            )a1
            inner join
            $dim_map_country_sdk a2 on a1.country = a2.ch_name
          )a
          group by device,country,country_code
        )aa
        lateral view explode(split(tag_list,',')) t as tag
      )aaa
    )aaaa
    where rank = 1
  )b1
)code_map
where country_code not in ('00','an','cs','eh');
"

echo "over..."

end_time=$(date +%s)

cost_time=$((end_time - start_time))

echo "This script runtime is $cost_time s"

