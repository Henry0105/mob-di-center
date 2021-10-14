#!/bin/bash
: '
@owner:liuyanqiang
@describe:计算bssid在各信号等级下连接的概率，每两周运行一次
@projectName:mobdi
'

set -e -x

day=$1
p1monthday=`date -d "$day -1 months" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh


#input
#dwd_wifilist_explore_sec_di=dm_mobdi_master.dwd_wifilist_explore_sec_di
tmpdb="$dm_mobdi_tmp"
#tmp
bssid_level_connect_lag_and_lead_info=$tmpdb.bssid_level_connect_lag_and_lead_info
bssid_level_connect_info=$tmpdb.bssid_level_connect_info
bssid_level_connect_smooth_info=$tmpdb.bssid_level_connect_smooth_info
level_connect_lag_and_lead_info=$tmpdb.level_connect_lag_and_lead_info
level_connect_probability=$tmpdb.level_connect_probability
bssid_level_connect_probability=$tmpdb.bssid_level_connect_probability
#output
#dim_bssid_level_connect_probability_all_mf=dim_mobdi_mapping.dim_bssid_level_connect_probability_all_mf

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--查看一个月bssid在不同信号强度下连接与未连接时的嗅探信息
insert overwrite table $bssid_level_connect_info partition(day='$day')
select bssid,
       if(connected='true',1,0) as connect_flag,
       level,
       count(1) as bssid_level_num
from $dwd_wifilist_explore_sec_di
where day>'$p1monthday'
and day<='$day'
and level>=-99
and level<0
and trim(bssid) not in ('','00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
group by bssid,if(connected='true',1,0),level;

--计算同一个连接情况的bssid在前后三个信号强度的嗅探次数
insert overwrite table $bssid_level_connect_lag_and_lead_info partition(day='$day')
select bssid,connect_flag,level,bssid_level_num,
       nvl(lag(bssid_level_num,1,0) over (partition by bssid,connect_flag order by level),0) as num_lag1,
       nvl(lag(bssid_level_num,2,0) over (partition by bssid,connect_flag order by level),0) as num_lag2,
       nvl(lag(bssid_level_num,3,0) over (partition by bssid,connect_flag order by level),0) as num_lag3,
       nvl(lead(bssid_level_num,1,0) over (partition by bssid,connect_flag order by level),0) as num_lead1,
       nvl(lead(bssid_level_num,2,0) over (partition by bssid,connect_flag order by level),0) as num_lead2,
       nvl(lead(bssid_level_num,3,0) over (partition by bssid,connect_flag order by level),0) as num_lead3
from
(
  select a.bssid,a.connect_flag,a.level,
         nvl(b.bssid_level_num,0) as bssid_level_num
  from
  (
    select bssid,connect_flag,level
    from
    (
      select bssid,
             connect_flag
      from $bssid_level_connect_info
      where day='$day'
      group by bssid,connect_flag
    ) t1
    LATERAL VIEW explode(array(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10,
                               -11,-12,-13,-14,-15,-16,-17,-18,-19,-20,
                               -21,-22,-23,-24,-25,-26,-27,-28,-29,-30,
                               -31,-32,-33,-34,-35,-36,-37,-38,-39,-40,
                               -41,-42,-43,-44,-45,-46,-47,-48,-49,-50,
                               -51,-52,-53,-54,-55,-56,-57,-58,-59,-60,
                               -61,-62,-63,-64,-65,-66,-67,-68,-69,-70,
                               -71,-72,-73,-74,-75,-76,-77,-78,-79,-80,
                               -81,-82,-83,-84,-85,-86,-87,-88,-89,-90,
                               -91,-92,-93,-94,-95,-96,-97,-98,-99)) n as level
  ) a
  left join
  $bssid_level_connect_info b
  on b.day='$day' and a.bssid=b.bssid and a.connect_flag=b.connect_flag and a.level=b.level
) t;

--对bsiid连接/未连接情况下每个信号强度的连接次数进行平滑
insert overwrite table $bssid_level_connect_smooth_info partition(day='$day')
select nvl(a.bssid,b.bssid) as bssid,
       nvl(a.level,b.level) as level,
       nvl(a.bssid_level_num,0) as collect_bssid_num,
       nvl(b.bssid_level_num,0) as uncollect_bssid_num,
       nvl(a.bssid_level_num_avg,0) as collect_bssid_num_avg,
       nvl(b.bssid_level_num_avg,0) as uncollect_bssid_num_avg
from
(
  select bssid,level,bssid_level_num,
         case
           when level>=-96 and level<=-4 then round((bssid_level_num+num_lag1+num_lag2+num_lag3+num_lead1+num_lead2+num_lead3)/7,2)
           when level<-96 then round((bssid_level_num+num_lead1+num_lead2+num_lead3)/4,2)
           when level>-4 then round((bssid_level_num+num_lag1+num_lag2+num_lag3)/4,2)
         end as bssid_level_num_avg
  from $bssid_level_connect_lag_and_lead_info
  where day='$day'
  and connect_flag=1
)a
full join
(
  select bssid,level,bssid_level_num,
         case
           when level>=-96 and level<=-4 then round((bssid_level_num+num_lag1+num_lag2+num_lag3+num_lead1+num_lead2+num_lead3)/7,2)
           when level<-96 then round((bssid_level_num+num_lead1+num_lead2+num_lead3)/4,2)
           when level>-4 then round((bssid_level_num+num_lag1+num_lag2+num_lag3)/4,2)
         end as bssid_level_num_avg
  from $bssid_level_connect_lag_and_lead_info
  where day='$day'
  and connect_flag=0
)b on a.bssid=b.bssid and a.level=b.level
cluster by bssid;
"

hive -v -e"
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
--同一个连接情况的信号强度在前后三个信号强度的嗅探次数
insert overwrite table $level_connect_lag_and_lead_info partition(day='$day')
select level,connect_flag,level_num,
       nvl(lag(level_num,1,0) over (partition by connect_flag order by level),0) as level_num_lag1,
       nvl(lag(level_num,2,0) over (partition by connect_flag order by level),0) as level_num_lag2,
       nvl(lag(level_num,3,0) over (partition by connect_flag order by level),0) as level_num_lag3,
       nvl(lead(level_num,1,0) over (partition by connect_flag order by level),0) as level_num_lead1,
       nvl(lead(level_num,2,0) over (partition by connect_flag order by level),0) as level_num_lead2,
       nvl(lead(level_num,3,0) over (partition by connect_flag order by level),0) as level_num_lead3
from
(
  select a.level,a.connect_flag,nvl(b.level_num,0) as level_num
  from
  (
    select level,connect_flag
    from
    (
      select 1 as connect_flag

      union all

      select 0 as connect_flag
    ) t1
    LATERAL VIEW explode(array(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10,
                               -11,-12,-13,-14,-15,-16,-17,-18,-19,-20,
                               -21,-22,-23,-24,-25,-26,-27,-28,-29,-30,
                               -31,-32,-33,-34,-35,-36,-37,-38,-39,-40,
                               -41,-42,-43,-44,-45,-46,-47,-48,-49,-50,
                               -51,-52,-53,-54,-55,-56,-57,-58,-59,-60,
                               -61,-62,-63,-64,-65,-66,-67,-68,-69,-70,
                               -71,-72,-73,-74,-75,-76,-77,-78,-79,-80,
                               -81,-82,-83,-84,-85,-86,-87,-88,-89,-90,
                               -91,-92,-93,-94,-95,-96,-97,-98,-99)) n as level
  ) a
  left join
  (
    select connect_flag,level,sum(bssid_level_num) as level_num
    from $bssid_level_connect_info
    where day='$day'
    group by connect_flag,level
  ) b
  on a.level=b.level and a.connect_flag=b.connect_flag
) t;

--计算各信号等级的连接概率
insert overwrite table $level_connect_probability partition(day='$day')
select level,
       case
         when level<=-10 then level_avg
         else 0.5582-(level+10)*0.005
       end as level_avg
from
(
  select level,
         round(collect_level_num_avg/(collect_level_num_avg+uncollect_level_num_avg),4) as level_avg
  from
  (
    select nvl(a.level,b.level) as level,
           nvl(a.level_num_avg,0) as collect_level_num_avg,
           nvl(b.level_num_avg,0) as uncollect_level_num_avg
    from
    (
      select level,
             case
                when level>=-96 and level<=-4 then round((level_num+level_num_lag1+level_num_lag2+level_num_lag3+level_num_lead1+level_num_lead2+level_num_lead3)/7,2)
                when level<-96 then round((level_num+level_num_lead1+level_num_lead2+level_num_lead3)/4,2)
                when level>-4 then round((level_num+level_num_lag1+level_num_lag2+level_num_lag3)/4,2)
              end as level_num_avg
      from $level_connect_lag_and_lead_info
      where day='$day'
      and connect_flag=1
    )a
    full join
    (
      select level,
             case
                when level>=-96 and level<=-4 then round((level_num+level_num_lag1+level_num_lag2+level_num_lag3+level_num_lead1+level_num_lead2+level_num_lead3)/7,2)
                when level<-96 then round((level_num+level_num_lead1+level_num_lead2+level_num_lead3)/4,2)
                when level>-4 then round((level_num+level_num_lag1+level_num_lag2+level_num_lag3)/4,2)
              end as level_num_avg
      from $level_connect_lag_and_lead_info
      where day='$day'
      and connect_flag=0
    )b on a.level=b.level
  )c
)d;

--计算bssid各信号等级的连接概率
insert overwrite table $bssid_level_connect_probability partition(day='$day')
select bssid,b.level,
       nvl(round(collect_bssid_num_avg/(collect_bssid_num_avg+uncollect_bssid_num_avg),4),a.level_avg) as collect_probability
from $level_connect_probability a
inner join
$bssid_level_connect_smooth_info b on b.day='$day' and a.level=b.level
where a.day='$day'
cluster by bssid;
"

#计算dim_bssid_level_connect_probability_all_mf表小于day最近的一个分区
lastPartition=`hive -e "show partitions $dim_bssid_level_connect_probability_all_mf" | awk -v day=${day} -F '=' '$2<day {print $0}'| sort| tail -n 1`
p3monthday=`date -d "$day -3 months" +%Y%m%d`

#合并近三个月全量数据
hive -v -e"
insert overwrite table $dim_bssid_level_connect_probability_all_mf partition(day='$day')
select bssid,calculate_day,level,collect_probability
from
(
  select bssid,calculate_day,level,collect_probability,
         row_number() over(partition by bssid,level order by calculate_day desc) rn
  from
  (
    select bssid,day as calculate_day,level,collect_probability
    from $bssid_level_connect_probability
    where day='$day'

    union all

    select bssid,calculate_day,level,collect_probability
    from $dim_bssid_level_connect_probability_all_mf
    where $lastPartition
    and calculate_day>'$p3monthday'
  ) t1
) t2
where rn=1
cluster by bssid;
"