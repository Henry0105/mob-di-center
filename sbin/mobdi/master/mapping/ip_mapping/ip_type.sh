#!/bin/bash
: '
@owner:liuyanqiang
@describe:ip属性识别
@projectName:mobdi
@BusinessName:IpMapping
'

set -e -x

day=$1
p3month=`date -d "$day -3 months" "+%Y%m%d"`
p1month=`date -d "$day -1 months" "+%Y%m%d"`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#源表
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
#dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di

#目标表
#dim_ip_type_all_mf=dm_mobdi_mapping.dim_ip_type_all_mf

#中间库
calculate_ip_type_base_info_by_location_info=dw_mobdi_tmp.calculate_ip_type_base_info_by_location_info
calculate_ip_type_base_info=dw_mobdi_tmp.calculate_ip_type_base_info
calculate_ip_type_connect_info=dw_mobdi_tmp.calculate_ip_type_connect_info
calculate_ip_type_connect_info_in_ip_bssid_dimension=dw_mobdi_tmp.calculate_ip_type_connect_info_in_ip_bssid_dimension
ip_stable_type=dw_mobdi_tmp.ip_stable_type
calculate_ip_abnormal_type_base_info=dw_mobdi_tmp.calculate_ip_abnormal_type_base_info
ip_abnormal_type=dw_mobdi_tmp.ip_abnormal_type
ip_dynamic_type=dw_mobdi_tmp.ip_dynamic_type

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $calculate_ip_type_base_info partition(day='$day')
select ipaddr,bssid,min(ssid) as ssid,clientdate,
       count(1) as connect_num
from
(
  select ipaddr,bssid,ssid,clientdatetime,
          to_date(clientdatetime) as clientdate
  from
  (
    select trim(bssid) as bssid,
           ssid,
           from_unixtime(floor(datetime/1000),'yyyy-MM-dd HH:mm:ss') as clientdatetime,
           ipaddr
    from $dwd_log_wifi_info_sec_di
    where day >= '$p3month'
    and day < '$day'
    and networktype = 'wifi'
    and trim(bssid) not in ('00:00:00:00:00:00', '02:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff')
    and trim(bssid) is not null
    and regexp_replace(trim(lower(bssid)), '-|:|\\\\.|\073', '') rlike '^[0-9a-f]{12}$'
    and unix_timestamp(serdatetime)-floor(datetime/1000)<=120
    and ipaddr rlike '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'
  ) t1
  group by ipaddr,bssid,ssid,clientdatetime
) t2
group by ipaddr,bssid,clientdate;

--首先计算三个月内ip+bssid维度的连接天数、最大连接日期、最小连接日期、最大最小连接日期差值、连接次数
--如果同一个ip有多个bssid对应，则依据连接天数、最大最小连接日期之间的差值、连接次数进行排序，取最大的一个ip
insert overwrite table $calculate_ip_type_connect_info partition(day='$day')
select ipaddr,bssid,active_days_ip_bssid,date_max_ip_bssid,date_min_ip_bssid,days_diff_ip_bssid,ssid_array
from
(
  select ipaddr,bssid,active_days_ip_bssid,date_max_ip_bssid,date_min_ip_bssid,days_diff_ip_bssid,connect_num,ssid_array,
         row_number() over (partition by ipaddr order by active_days_ip_bssid desc,days_diff_ip_bssid desc,connect_num desc) as rn
  from
  (
    select ipaddr,
           bssid,
           count(1) as active_days_ip_bssid,
           max(clientdate) as date_max_ip_bssid,
           min(clientdate) as date_min_ip_bssid,
           datediff(max(clientdate),min(clientdate)) as days_diff_ip_bssid,
           sum(connect_num) as connect_num,
           collect_set(ssid) as ssid_array
    from $calculate_ip_type_base_info
    where day='$day'
    group by ipaddr,bssid
  ) b
) c
where rn=1;

--得到ip+bssid维度的连接天数，最大连接日期、最小连接日期、最大最小连接日期差值
--bssid维度的连接天数，最大连接日期、最小连接日期、最大最小连接日期差值
--ip维度的连接天数，最大连接日期、最小连接日期、最大最小连接日期差值
--最后计算ip+bssid维度连接天数分别在bssid维度和ip维度的连接天数的占比
--以及ip+bssid维度最大最小连接日期差值分别在bssid维度和ip维度的最大最小连接日期差值的占比
insert overwrite table $calculate_ip_type_connect_info_in_ip_bssid_dimension partition(day='$day')
select a.ipaddr,a.bssid,a.ssid_array,
       a.active_days_ip_bssid,
       b.active_days_bssid,
       c.active_days_ip,
       a.date_max_ip_bssid,
       a.date_min_ip_bssid,
       b.date_max_bssid,
       b.date_min_bssid,
       c.date_max_ip,
       c.date_min_ip,
       a.days_diff_ip_bssid,
       b.days_diff_bssid,
       c.days_diff_ip,
       round(nvl(a.active_days_ip_bssid/b.active_days_bssid,0),4) as rate_days_bssid,
       round(nvl(a.active_days_ip_bssid/c.active_days_ip,0),4) as rate_days_ip,
       round(nvl(a.days_diff_ip_bssid/b.days_diff_bssid,0),4) as rate_diff_bssid,
       round(nvl(a.days_diff_ip_bssid/c.days_diff_ip,0),4) as rate_diff_ip
from $calculate_ip_type_connect_info a
left join
(
  select bssid,
         count(1) as active_days_bssid,
         max(clientdate) as date_max_bssid,
         min(clientdate) as date_min_bssid,
         datediff(max(clientdate),min(clientdate)) as days_diff_bssid
  from
  (
    select bssid,clientdate
    from $calculate_ip_type_base_info
    where day='$day'
    group by bssid,clientdate
  )b1
  group by bssid
) b on a.bssid=b.bssid
left join
(
  select ipaddr,
         count(1) as active_days_ip,
         max(clientdate) as date_max_ip,
         min(clientdate) as date_min_ip,
         datediff(max(clientdate),min(clientdate)) as days_diff_ip
  from
  (
    select ipaddr,clientdate
    from $calculate_ip_type_base_info
    where day='$day'
    group by ipaddr,clientdate
  )c1
  group by ipaddr
) c on a.ipaddr=c.ipaddr
where a.day='$day';

--ip+bssid维度的最大最小连接日期差值>45天
--条件1
--ip+bssid维度的最大最小连接日期差值=bssid维度的最大最小连接日期差值
--ip+bssid维度的最大最小连接日期差值=ip维度的最大最小连接日期差值
--条件2
--(0<bssid维度的最大最小连接日期差值 - ip+bssid维度的最大最小连接日期差值<=2)并且(0<ip维度的最大最小连接日期差值 - ip+bssid维度的最大最小连接日期差值<=2)
--或者(0.85<=ip+bssid维度最大最小连接日期差值分别在ip维度的最大最小连接日期差值的占比<1.0 并且 ip+bssid维度最大最小连接日期差值分别在bssid维度的最大最小连接日期差值的占比=1.0)
--或者(0.85<=ip+bssid维度最大最小连接日期差值分别在bssid维度的最大最小连接日期差值的占比<1.0 并且 ip+bssid维度最大最小连接日期差值分别在ip维度的最大最小连接日期差值的占比=1.0)
insert overwrite table $ip_stable_type partition(day='$day')
select ipaddr,bssid,ssid_array
from
(
  select ipaddr,bssid,ssid_array
  from $calculate_ip_type_connect_info_in_ip_bssid_dimension
  where day='$day'
  and days_diff_ip_bssid>=45
  and days_diff_ip_bssid=days_diff_bssid
  and days_diff_ip_bssid=days_diff_ip

  union all

  select ipaddr,bssid,ssid_array
  from $calculate_ip_type_connect_info_in_ip_bssid_dimension
  where day='$day'
  and days_diff_ip_bssid>=45
  and ((days_diff_bssid-days_diff_ip_bssid<=2
          and days_diff_ip-days_diff_ip_bssid<=2
          and days_diff_ip_bssid!=days_diff_bssid
          and days_diff_ip_bssid!=days_diff_ip)
        or (rate_diff_ip>=0.85 and rate_diff_ip<1.0 and rate_diff_bssid=1.0)
        or (rate_diff_ip=1.0 and rate_diff_bssid>=0.85 and rate_diff_bssid<1.0))
) a
group by ipaddr,bssid,ssid_array;
"

:<<!
CREATE TABLE `calculate_ip_type_base_info`(
  `ipaddr` string COMMENT 'IP地址',
  `bssid` string COMMENT 'bssid',
  `ssid` string COMMENT 'ssid',
  `clientdate` string COMMENT '客户端日期',
  `connect_num` int COMMENT '连接次数')
COMMENT '计算ip属性用的基础信息'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.calculate_ip_type_connect_info`(
  `ipaddr` string COMMENT 'IP地址',
  `bssid` string COMMENT 'bssid',
  `active_days_ip_bssid` int COMMENT 'ip+bssid维度的连接天数',
  `date_max_ip_bssid` string COMMENT 'ip+bssid维度的最大连接日期',
  `date_min_ip_bssid` string COMMENT 'ip+bssid维度的最小连接日期',
  `days_diff_ip_bssid` int COMMENT 'ip+bssid维度的最大最小连接日期差值',
  `ssid_array` array<string> COMMENT 'ssid数组')
COMMENT '计算三个月内ip+bssid维度的连接天数、最大连接日期、最小连接日期、最大最小连接日期差值'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.calculate_ip_type_connect_info_in_ip_bssid_dimension`(
  `ipaddr` string COMMENT 'IP地址',
  `bssid` string COMMENT 'bssid',
  `ssid_array` array<string> COMMENT 'ssid数组',
  `active_days_ip_bssid` int COMMENT 'ip+bssid维度的连接天数',
  `active_days_bssid` int COMMENT 'bssid维度的连接天数',
  `active_days_ip` int COMMENT 'ip维度的连接天数',
  `date_max_ip_bssid` string COMMENT 'ip+bssid维度的最大连接日期',
  `date_min_ip_bssid` string COMMENT 'ip+bssid维度的最小连接日期',
  `date_max_bssid` string COMMENT 'bssid维度的最大连接日期',
  `date_min_bssid` string COMMENT 'bssid维度的最小连接日期',
  `date_max_ip` string COMMENT 'ip维度的最大连接日期',
  `date_min_ip` string COMMENT 'ip维度的最小连接日期',
  `days_diff_ip_bssid` int COMMENT 'ip+bssid维度的最大最小连接日期差值',
  `days_diff_bssid` int COMMENT 'bssid维度的最大最小连接日期差值',
  `days_diff_ip` int COMMENT 'ip维度的最大最小连接日期差值',
  `rate_days_bssid` double COMMENT 'ip+bssid维度连接天数在bssid维度3的连接天数的占比',
  `rate_days_ip` double COMMENT 'ip+bssid维度连接天数在ip维度的连接天数的占比',
  `rate_diff_bssid` double COMMENT 'ip+bssid维度最大最小连接日期差值在bssid维度的最大最小连接日期差值的占比',
  `rate_diff_ip` double COMMENT 'ip+bssid维度最大最小连接日期差值在ip维度的最大最小连接日期差值的占比')
COMMENT '计算三个月内ip+bssid维度、bssid维度、ip维度的连接信息'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.ip_stable_type`(
  `ipaddr` string COMMENT 'IP地址',
  `bssid` string COMMENT 'bssid',
  `ssid_array` array<string> COMMENT 'ssid数组')
COMMENT '静态ip结果表'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.calculate_ip_type_base_info_by_location_info`(
  `deviceid` string COMMENT '设备标示',
  `clientip` string COMMENT 'IP地址',
  `longitude` float COMMENT '经度',
  `latitude` float COMMENT '纬度')
COMMENT '计算ip属性用的基础信息'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.calculate_ip_abnormal_type_base_info`(
  `clientip` string COMMENT 'IP地址',
  `rate_1km` double COMMENT '1km内覆盖率',
  `rate_100km` double COMMENT '100km内覆盖率',
  `rate_200km` double COMMENT '200km内覆盖率',
  `rate_300km` double COMMENT '300km内覆盖率')
COMMENT '计算异常ip属性用的基础信息'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.ip_abnormal_type`(
  `clientip` string COMMENT 'IP地址')
COMMENT '异常ip结果表'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dw_mobdi_md.ip_dynamic_type`(
  `clientip` string COMMENT 'IP地址')
COMMENT '动态ip结果表'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;

CREATE TABLE `dm_mobdi_mapping.dim_ip_type_all_mf`(
  `clientip` string COMMENT 'IP地址',
  `type` int COMMENT 'ip类型，1-动态ip，2-静态ip，3-异常ip')
COMMENT 'ip属性全量表，不在表中的ip属于无法识别'
PARTITIONED BY (
  `day` string COMMENT '日期')
stored as orc;
!

#地理位置异常的ip
#上传时间间隔1分钟内
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $calculate_ip_type_base_info_by_location_info partition(day='$day')
select muid as deviceid,trim(clientip) as clientip,longitude,latitude
from $dwd_location_info_sec_di
where day >= '$p1month'
and day < '$day'
and networktype = 'wifi'
and serdatetime-clienttime<=60000
and longitude > 73
and longitude < 136
and latitude > 3
and latitude < 54
and muid rlike '^[0-9a-f]{40}$'
and trim(clientip) rlike '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'
group by muid,trim(clientip),longitude,latitude;
"

hive -v -e "
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $calculate_ip_abnormal_type_base_info partition(day='$day')
select clientip,
       round(nvl(sum(if(distance<=1, 1, 0))/count(1),0),4) as rate_1km,
       round(nvl(sum(if(distance<=100, 1, 0))/count(1),0),4) as rate_100km,
       round(nvl(sum(if(distance<=200, 1, 0))/count(1),0),4) as rate_200km,
       round(nvl(sum(if(distance<=300, 1, 0))/count(1),0),4) as rate_300km
from
(
  select a.deviceid,a.clientip,
         cast(get_distance(a.latitude,a.longitude,b.latitude_avg,b.longitude_avg)/1000 as int) as distance
  from $calculate_ip_type_base_info_by_location_info a
  inner join
  (
    select clientip,
           percentile(cast(longitude*100000 as bigint),0.5)/100000 as longitude_avg,
           percentile(cast(latitude*100000 as bigint),0.5)/100000 as latitude_avg
    from $calculate_ip_type_base_info_by_location_info
    where day='$day'
    group by clientip
    having count(1)>=5
  ) b on a.clientip=b.clientip
  where a.day='$day'
) a
group by clientip;

insert overwrite table $ip_abnormal_type partition(day='$day')
select clientip
from $calculate_ip_abnormal_type_base_info
where day='$day'
and rate_300km<0.4;
"

#排除静态ip和异常ip，都是动态ip
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $ip_dynamic_type partition(day='$day')
select t1.clientip
from
(
  select clientip
  from $calculate_ip_type_base_info_by_location_info
  where day='$day'
  group by clientip
) t1
left join
(
  select clientip
  from
  (
    select ipaddr as clientip
    from $ip_stable_type
    where day='$day'

    union all

    select clientip
    from $ip_abnormal_type
    where day='$day'
  ) t
  group by clientip
) t2 on t1.clientip=t2.clientip
where t2.clientip is null;
"

: '
增量表合并到全量表的计算规则如下
更新优先级：异常ip>静态ip>动态ip
全量表中为动态ip，若增量表中为异常ip或者静态ip，更新时以增量表数据为准；
全量表中为静态ip，若增量表中为异常ip或者连续三个月为动态ip，更新时以增量表数据为准，否则为静态ip；
全量表中为异常ip，若连续三个月的增量表中为静态ip或者连续三个月为动态ip，更新时以增量表数据为准，否则为异常ip
'
#计算dm_mobdi_mapping.dim_ip_type_all_mf表小于day最近的一个分区
lastPartition=`hive -e "show partitions $dim_ip_type_all_mf" | awk -v day=${day} -F '=' '$2<day {print $0}'| sort| tail -n 1`
#计算小于等于day最近的三个分区，并用' or '连接
newestThreePartitions=`hive -e "show partitions $ip_stable_type" | awk -v day=${day} -F '=' '$2<=day {print $0}'| sort| tail -n 3| xargs echo| sed 's/\s/ or /g'`
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $dim_ip_type_all_mf partition(day='$day')
select ful.clientip,
       case
         when ful.type=2 and dynamic_3month.clientip is not null then 1
         when ful.type=3 and dynamic_3month.clientip is not null then 1
         when ful.type=3 and stable_3month.clientip is not null then 2
         else ful.type
       end as type
from
(
  select clientip,max(type) as type
  from
  (
    select clientip,type from $dim_ip_type_all_mf where $lastPartition

    union all

    select clientip, 1 as type from $ip_dynamic_type where day='$day'

    union all

    select ipaddr as clientip, 2 as type from $ip_stable_type where day='$day'

    union all

    select clientip, 3 as type from $ip_abnormal_type where day='$day'
  )  t1
  group by clientip
) ful
left join
(
  --连续三个月为静态ip
  SELECT ipaddr as clientip
  from $ip_stable_type where $newestThreePartitions
  group by ipaddr
  having count(1)=3
) stable_3month on ful.clientip=stable_3month.clientip
left join
(
  --连续三个月为动态ip
  SELECT clientip
  from $ip_dynamic_type where $newestThreePartitions
  group by clientip
  having count(1)=3
) dynamic_3month on ful.clientip=dynamic_3month.clientip;
"