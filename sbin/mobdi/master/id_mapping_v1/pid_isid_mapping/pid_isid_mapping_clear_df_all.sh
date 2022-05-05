#!/bin/sh

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <day>"
  exit 1
fi

day=$1

:<<!
create table dim_mobdi_mapping.pid_isid_mapping_clear_df
(
  pid string,
  isid string,
  carrier string,
  first_time string,
  last_time string,
  flag string)
partitioned by (day string)
row format delimited fields terminated by '\t'
!


# input
dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di
dim_pid_attribute_full_par_secview=dm_mobdi_mapping.dim_pid_attribute_full_par_secview
dim_isid_attribute_full_par_secview=dm_mobdi_mapping.dim_isid_attribute_full_par_secview
# output
jh_pid_isid=dm_mobdi_tmp.jh_pid_isid
jh_pid_isid_withoutsim=dm_mobdi_tmp.jh_pid_isid_withoutsim
pid_isid_mapping_df_pre=dm_mobdi_tmp.pid_isid_mapping_df_pre
pid_isid_mapping_clear_df=dim_mobdi_mapping.pid_isid_mapping_clear_df

#数据2019.1101-2022.0413
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
    , t2.pid_carrier 
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
                  day>='20191101' 
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
                    where country = '中国' and  province not in ('香港','澳门','台湾')
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
    , t2.pid_carrier
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
              day>='20191101' and day<='${day}'
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
hive -v -e "$sql2";

#全量pre数据,取最早最晚时间和最小flag
sql3="
$sqlset

drop table if exists ${pid_isid_mapping_df_pre};
create table ${pid_isid_mapping_df_pre} as
select pid
     , isid
     , pid_carrier
     , isid_carrier
     , min(first_time) as first_time
     , max(last_time) as last_time
     , min(flag) as flag
from (
  select * from ${jh_isid_pid}
  union all
  select * from ${jh_pid_isid_withoutsim}
  ) a 
group by pid
       , isid
       , pid_carrier
       , isid_carrier

"
hive -v -e "$sql3";


#一个isid最多对应2个pid（存在一卡双号的业务）,一个pid最多对应2个isid（存在一卡双号的业务）
#一个pid对应超过2个isid，删掉这个pid关联的全部，一个imsi对应超过两个pid，删掉这个isid关联的全部数据
sql4="
$sqlset

insert into table ${pid_isid_mapping_clear_df} partition(day='${day}')
select t1.pid,t1.isid,t1.pid_carrier as carrier,t1.first_time,t1.last_time,t1.flag
from ${pid_isid_mapping_df_pre} as t1
inner join 
(
  select pid
  from ${pid_isid_mapping_df_pre}
  group by pid
  having count(*) <= 2
) as t2 
on t1.pid = t2.pid
inner join 
(
  select isid
  from ${pid_isid_mapping_df_pre}
  group by isid
  having count(*) <= 2
) as t3
on t1.isid = t3.isid
"
hive -v -e "$sql4";

