#!/bin/sh

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1
daybefore7=`date -d "$day -7 days" "+%Y%m%d"`

:<<!
create table dim_mobdi_mapping.pid_isid_mapping_clear_df
(
  pid string,
  isid string,
  carrier string comment "设备手机运营商",
  first_time string comment "最早服务器采集时间",
  last_time string comment "最新服务器采集时间",
  flag string comment "flag：1-pid和isid都来自sim卡，2-仅isid来自sim卡pid为api接口调用，3-仅pid来自sim卡isid为api接口调用，4-pid和isid均为api接口调用"
  )
partitioned by (day string)
row format delimited fields terminated by ','
stored as orc;
!


# input
dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di
dim_pid_attribute_full_par_secview=dm_mobdi_mapping.dim_pid_attribute_full_par_secview
dim_isid_attribute_full_par_secview=dm_mobdi_mapping.dim_isid_attribute_full_par_secview
# output
jh_pid_isid=dm_mobdi_tmp.jh_pid_isid
jh_pid_isid_withoutsim=dm_mobdi_tmp.jh_pid_isid_withoutsim
pid_isid_mapping_di_pre=dm_mobdi_tmp.pid_isid_mapping_di_pre
pid_isid_mapping_clear_di=dm_mobdi_tmp.pid_isid_mapping_clear_di
pid_isid_mapping_clear_df=dim_mobdi_mapping.pid_isid_mapping_clear_df

lastpar=`hive -e "show partitions ${pid_isid_mapping_clear_df}" | awk -F '=' '{print $2}' | grep -v '_bak' | tail -n 1`

sqlset="
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.map.aggr=true;
set hive.auto.convert.join=true;
set hive.groupby.skewindata=true;
set mapreduce.job.queuename=root.yarn_data_compliance; 
"

sql1="
$sqlset

drop table if exists ${jh_pid_isid};
create table ${jh_pid_isid} as
select pid
     , isid
     , pid_carrier
     , isid_carrier
     , first_time
     , last_time
     , flag
from (
    select
      pid_sim as pid
    , isid_sim as isid
    , t2.pid_carrier as pid_carrier
    , t3.isid_operator as isid_carrier
    , first_time
    , last_time
    , flag
    from
      (
        select
          if(isid_sim  = '', isid, isid_sim)   as isid_sim
        , if(pid_sim = '', pid, pid_sim) as pid_sim
        , min(serdatetime) as first_time
        , max(serdatetime) as last_time
        , case when (pid_sim<>'' and pid_sim is not null) and (isid_sim<>'' and isid_sim is not null) then 1
               when (pid_sim='' or pid_sim is null) and (isid_sim<>'' and isid_sim is not null) then 2
               when (pid_sim<>'' and pid_sim is not null) and (isid_sim='' or isid_sim is  null) then 3
               when (pid_sim='' or pid_sim is null) and (isid_sim='' or isid_sim is  null) then 4
           end as flag
        from
          (
            select
              pid
            , isid
            , nvl(simlist1.isid,'') as isid_sim
            , nvl(simlist1.pid,'') as pid_sim
            , serdatetime
            from
              (
                select
                  pid
                , isid
                , serdatetime
                , sims.simlist as simlist
                from ${dwd_log_device_info_jh_sec_di}
                where
                  day>'${daybefore7}' 
                  and day<='${day}'
                  and plat = '1'
              )
              as a lateral view explode(simlist) mytable as simlist1
          ) t
        group by
          if(isid_sim  = '', isid, isid_sim)
        , if(pid_sim = '', pid, pid_sim)
        ,case when (pid_sim<>'' and pid_sim is not null) and (isid_sim<>'' and isid_sim is not null) then 1
              when (pid_sim='' or pid_sim is null) and (isid_sim<>'' and isid_sim is not null) then 2
              when (pid_sim<>'' and pid_sim is not null) and (isid_sim='' or isid_sim is  null) then 3
              when (pid_sim='' or pid_sim is null) and (isid_sim='' or isid_sim is  null) then 4
         end
      ) t1
      inner join (select pid_id
                        ,case when carrier in ('移动','联通','电信') then carrier 
                              else '其他' 
                         end as pid_carrier
                    from ${dim_pid_attribute_full_par_secview}
                        where country = '中国'
                        and  province not in ('香港','澳门','台湾')
       ) t2 on t1.pid_sim = t2.pid_id
      left join (select isid
                       ,case
                           when operator = 'CHINA RAILCOM'
                             then '移动'
                           when operator = 'CHINA MOBILE'
                             then '移动'
                           when operator = 'CHINA UNICOM'
                             then '联通'
                           when operator = 'CHINA TELECOM'
                             then '电信'
                             else '其他'
                        end as isid_operator
                 from ${dim_isid_attribute_full_par_secview}
                 where country='中国' 
       )t3 on t1.isid_sim = t3.isid
    ) a
where
  pid_carrier = isid_carrier
  and pid_carrier <> '其他'
  and isid_carrier  <> '其他'
"
hive -v -e "$sql1";

sql2="
$sqlset

drop table if exists ${jh_pid_isid_withoutsim};
create table ${jh_pid_isid_withoutsim} as
select
  pid
, isid
, pid_carrier
, isid_carrier
, first_time
, last_time
, flag
from (
    select
      pid
    , t1.isid
    , t2.pid_carrier as pid_carrier
    , t3.isid_operator as isid_carrier
    , first_time
    , last_time
    , 4 as flag
    from (
            select
              pid
            , isid
            , min(serdatetime) as first_time
            , max(serdatetime) as last_time
            from ${dwd_log_device_info_jh_sec_di}
            where
              day>'${daybefore7}' and day<='${day}'
              and plat = '1' and sims.simlist is null
            and length(pid)> 0
            and length(isid)>0
            group by isid, pid
        ) t1
      inner join (select pid_id
                        ,case when carrier in ('移动','联通','电信') then carrier 
                              else '其他' 
                         end as pid_carrier
                    from ${dim_pid_attribute_full_par_secview}
                        where country = '中国'
                        and  province not in ('香港','澳门','台湾')
       ) t2 on t1.pid = t2.pid_id
      left join (select isid
                       ,case
                           when operator = 'CHINA RAILCOM'
                             then '移动'
                           when operator = 'CHINA MOBILE'
                             then '移动'
                           when operator = 'CHINA UNICOM'
                             then '联通'
                           when operator = 'CHINA TELECOM'
                             then '电信'
                             else '其他'
                        end as isid_operator
                 from ${dim_isid_attribute_full_par_secview}
                        where country='中国' 
        )t3 on t1.isid = t3.isid
) a
where
     pid_carrier = isid_carrier
     and pid_carrier <> '其他'
     and isid_carrier  <> '其他'
"
hive -v -e "${sql2}";

#增量pre数据
#取最早最晚时间和最小flag
sql3="
$sqlset

drop table if exists ${pid_isid_mapping_di_pre};
create table ${pid_isid_mapping_di_pre} as
select pid
     , isid
     , pid_carrier
     , isid_carrier
     , min(first_time) as first_time
     , max(last_time) as last_time
     , min(flag) as flag
from (
  select * from ${jh_pid_isid}
  union all
  select * from ${jh_pid_isid_withoutsim} 
  ) a 
group by pid
       , isid
       , pid_carrier
       , isid_carrier
"
hive -v -e "${sql3}";

#增量数据清洗
sql4="
$sqlset

drop table if exists ${pid_isid_mapping_clear_di};
create table ${pid_isid_mapping_clear_di} as 
select t1.pid,t1.isid,t1.pid_carrier as carrier,t1.first_time,t1.last_time,t1.flag
from ${pid_isid_mapping_di_pre} as t1
inner join 
(
  select pid
  from ${pid_isid_mapping_di_pre}
  group by pid
  having count(*) <= 2
) as t2 
on t1.pid = t2.pid
inner join 
(
  select isid
  from ${pid_isid_mapping_di_pre}
  group by isid
  having count(*) <= 2
) as t3
on t1.isid = t3.isid
"
hive -v -e "${sql4}";

#更新全量数据：增量数据与全量数据合并，按flag顺序和last_time倒序，取pid和isid 2v2关系，删掉多余的条数，不删pid或isid关联的全部数据
sql5="
$sqlset

insert into table ${pid_isid_mapping_clear_df} partition(day='${day}')
select pid,isid,carrier,first_time,last_time,flag
from(
  select 
    pid,isid,carrier,first_time,last_time,flag,
    row_number() over (partition by isid order by flag asc, last_time desc, pid asc) as pid_rank
  from(
    select 
      pid,isid,carrier,first_time,last_time,flag,
      row_number() over (partition by pid order by flag asc, last_time desc, isid asc) as isid_rank
    from(
      select pid,isid,carrier,min(first_time) as first_time,max(last_time) as last_time,min(flag) as flag
      from(
        select pid,isid,carrier,first_time,last_time,flag from ${pid_isid_mapping_clear_df}
        where day='${lastpar}'
        union all 
        select pid,isid,carrier,first_time,last_time,flag from ${pid_isid_mapping_clear_di} 
      ) t
      group by pid,isid,carrier
    ) t1
  ) t2
  where isid_rank<=2
) t3
where pid_rank<=2
"
hive -v -e "${sql5}";

#保留每月第一个分区
sql6="
insert overwrite table ${pid_isid_mapping_clear_df} partition(day='${day}_monthly_bak')
select pid,isid,carrier,first_time,last_time,flag
from ${pid_isid_mapping_clear_df} 
where day='${day}';
"
if ((${daybefore7} < `date -d "${day} " +%Y%m01`)) ;then
hive -v -e "${sql6}"
fi

#保留最新5个分区
for old_version in `hive -e "show partitions ${pid_isid_mapping_clear_df} " | grep -v '_bak' | sort | head -n -5`
do
    hive -v -e "alter table ${pid_isid_mapping_clear_df} drop if exists partition($old_version)"
done




