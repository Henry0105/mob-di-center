#!/bin/bash
: '
@owner:luost
@describe:亲友/工作共网关系
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p3monthDay=`date -d "$day -3 months" "+%Y%m%d"`

source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties


#源表
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
#rp_device_location_3monthly=dm_mobdi_report.rp_device_location_3monthly

#mapping表
tmp_home_geohash_info=dm_mobdi_tmp.tmp_home_geohash_info
tmp_work_geohash_info=dm_mobdi_tmp.tmp_work_geohash_info
#dim_mapping_bssid_location_mf=dm_mobdi_mapping.dim_mapping_bssid_location_mf
#dim_traffic_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_traffic_ssid_bssid_match_info_mf
#dim_shopping_mall_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_shopping_mall_ssid_bssid_match_info_mf
#dim_hotel_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_hotel_ssid_bssid_match_info_mf
#dim_car_4s_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_car_4s_ssid_bssid_match_info_mf
#vacation_flag=dm_sdk_mapping.vacation_flag

#中间表
tmp_workday_bssid_device_info=dm_mobdi_tmp.tmp_workday_bssid_device_info
tmp_work_ssid_location_wt=dm_mobdi_tmp.tmp_work_ssid_location_wt
tmp_work_ssid_bssid_location_info=dm_mobdi_tmp.tmp_work_ssid_bssid_location_info
tmp_work_ssid_device_distance_confidence_info=dm_mobdi_tmp.tmp_work_ssid_device_distance_confidence_info

#输出表
#device_work_relation_info_mi=dm_mobdi_report.device_work_relation_info_mi

#工作一度关系中间表建立
hive -v -e "
create table if not exists $tmp_workday_bssid_device_info(
    bssid string comment 'bssid',
    device string comment '设备号',
    real_date string comment '日期'
)
comment '工作一度关系，工作日白天设备连接bssid信息中间表'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_work_ssid_location_wt(
    ssid string comment 'ssid',
    lat string comment '纬度',
    lon string comment '精度',
    ssid_name_wt string comment '工作一度关系，ssid通过名字匹配的权重中间表'
)
comment '工作一度关系，ssid权重信息'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_work_ssid_bssid_location_info(
    ssid string comment 'ssid',
    bssid string comment 'bssid',
    lat string comment '纬度',
    lon string comment '精度',
    lat_round string comment '保留三位小数的纬度',
    lon_round string comment '保留三位小数的精度',
    ssid_name_wt string comment '权重'
)
comment '工作一度关系,ssid、bssid位置和权重信息中间表'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_work_ssid_device_distance_confidence_info(
    device string comment '设备号',
    ssid string comment 'ssid',
    lat_round string comment '经度',
    lon_round string comment '纬度',
    distance string comment '距离',
    confidence string comment '置信度',
    bssid_days string comment '连接天数',
    ssid_name_wt string comment '权重'
)
comment '工作一度关系,device与ssid距离、连接天数和权重信息'
partitioned by (day string comment '日期')
stored as orc;
"

#输出表建立
hive -v -e "
create table if not exists $device_work_relation_info(
    device string comment '设备',
    device_work string comment '工作关系设备',
    confidence double comment '置信度,0-1,越大关系越强',
    work_type string comment '关系类型（0都不是公司，关系不确定；1有一方公司，可能是客户；2双方公司，同事）'
)
comment '工作共网关系'
partitioned by (day string comment '日期')
stored as orc;
"

: '
@part_1:
实现功能:生成一度工作关系设备网络信息
基本逻辑:
        1.筛选工作类的bssid。
        2.找出连接这些bssid的设备，根据连接天数和位置信息，计算bssid与设备之间的权重。
        3.对连接同一bssid的设备计算设备两两之间的权重。最后对边聚合权重。
        4.详细逻辑:http://c.mob.com/pages/viewpage.action?pageId=32316980
输出结果:
'

#记录三个月内在工作日白天出现的bssid、device和出现日期
hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;
set hive.exec.max.created.files=200000;

insert overwrite table $tmp_workday_bssid_device_info partition (day = '$day')
select bssid,device,real_date
from
(
    select device,bssid,ssid,day,
           from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date,
           from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'HH') as hours
     from $dwd_log_wifi_info_sec_di
     where day >= '$p3monthDay' 
     and day <= '$day' 
     and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') 
     and bssid is not null 
)x 
where real_date >= '$p3monthDay' 
and real_date <= '$day'
and pmod(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(real_date,'yyyyMMdd'))),'2018-01-01'),7) +1 not in (6,7)
and hours >= 8 
and hours <= 21
and not exists (select day from $vacation_flag v where x.real_date = v.day);
"

bssidMappingLastParStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`
trafficParition=`hive -e "show partitions $dim_traffic_ssid_bssid_match_info_mf" | sort| tail -n 1`
shoppingPartition=`hive -e "show partitions $dim_shopping_mall_ssid_bssid_match_info_mf" | sort| tail -n 1`
hotelPartition=`hive -e "show partitions $dim_hotel_ssid_bssid_match_info_mf" | sort| tail -n 1`
carPartition=`hive -e "show partitions $dim_car_4s_ssid_bssid_match_info_mf" | sort| tail -n 1`
hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;

--将所有bssid中去除已识别(交通、商场、旅馆、4s店)的bssid信息
with work_bssid_etlIdentifiedBssid_info as 
(
    select bssid_location.bssid,bssid_location.ssid,bssid_location.lat,bssid_location.lon
    from
    (
        select bssid,ssid,lat,lon
        from $dim_mapping_bssid_location_mf
        where $bssidMappingLastParStr 
        and bssid_type=1
    ) bssid_location
    left join
    (
        select bssid
        from $dim_traffic_ssid_bssid_match_info_mf
        lateral view explode(bssid_array) n as bssid
        where $trafficParition
    ) traffic
    on bssid_location.bssid = traffic.bssid
    left join
    (
        select bssid
        from $dim_shopping_mall_ssid_bssid_match_info_mf
        lateral view explode(bssid_array) n as bssid
        where $shoppingPartition
    ) shopping_mall
    on bssid_location.bssid = shopping_mall.bssid
    left join
    (
        select bssid
        from $dim_hotel_ssid_bssid_match_info_mf
        lateral view explode(bssid_array) n as bssid
        where $hotelPartition
    ) hotel
    on bssid_location.bssid = hotel.bssid
    left join
    (
        select bssid
        from $dim_car_4s_ssid_bssid_match_info_mf
        lateral view explode(bssid_array) n as bssid
        where $carPartition
    ) car_4s
    on bssid_location.bssid = car_4s.bssid
    where traffic.bssid is null
    and shopping_mall.bssid is null
    and hotel.bssid is null
    and car_4s.bssid is null
)

--对上面的bssid，计算同ssid+经纬度下，连接设备数量
--保留连接设备数量为3-10000且ssid名称不含两个以上的逗号，且包含中文或字母或数字的数据
--并标记包含某些比较像公共场所的词/比较像私人的词的ssid，设定降权系数0.5
insert overwrite table $tmp_work_ssid_location_wt partition (day = '$day')
select ssid,lat,lon,
       case 
         when lower(ssid) rlike '.*免费|free|i-|地铁|plaza|蹭网|上网|大厦|智慧城市|tp-|tenda|公寓|酒店|ktv|服务区|热线|unknown ssid|cmcc|chinanet|and|0x.*' then 0.5
         when ssid like '%??%' then 0.5
         else 1 end as ssid_name_wt
from
(
    select ssid,lat,lon,count(1) as dev_num
    from
    (
        select a.device,b.ssid,b.lat,b.lon
        from
        (
            select device,bssid
            from $tmp_workday_bssid_device_info
            where day = '$day'
            group by device,bssid
        )a
        inner join work_bssid_etlIdentifiedBssid_info b
        on a.bssid = b.bssid
        group by a.device,b.ssid,b.lat,b.lon
    )xx 
    group by ssid,lat,lon
)b
where dev_num >= 3 
and dev_num <= 10000
and trim(ssid) != ''
and ssid not like '%,%,%'
and regexp_replace(ssid,'[^A-Za-z0-9\\\\u4e00-\\\\u9fa5]','') != '';
"

hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';

--匹配小区,计算ssid与最近小区的距离
with ssid_home_distance_info as
(
    select x.ssid,x.lat,x.lon,x.ssid_name_wt,
           min(get_distance(x.lat,x.lon,y.lat,y.lon)) as min_home_dist
    from
    (
        select ssid,lat,lon,ssid_name_wt,get_geohash(lat,lon,6) as bssid_geohash6
        from $tmp_work_ssid_location_wt
        where day = '$day'
    )x
    inner join
    (
        select lat,lon,geohash6
        from $tmp_home_geohash_info
        where day = '$day' 
        and lat > 0 
        and lon > 0 
    )y
    on x.bssid_geohash6 = y.geohash6
    group by x.ssid,x.lat,x.lon,x.ssid_name_wt
),

--匹配写字楼,计算ssid与最近写字楼的距离
ssid_work_distance_info as 
(
    select x.ssid,x.lat,x.lon,x.ssid_name_wt,
           min(get_distance(x.lat,x.lon,y.lat,y.lon)) as min_office_dist
    from
    (
        select ssid,lat,lon,ssid_name_wt,get_geohash(lat,lon,6) as bssid_geohash6
        from $tmp_work_ssid_location_wt
        where day = '$day' 
    )x
    inner join
    (
        select lat,lon,geohash6
        from $tmp_work_geohash_info
        where day = '$day'
        and lat>0 
        and lon>0 
    )y
    on x.bssid_geohash6 = y.geohash6
    group by x.ssid,x.lat,x.lon,x.ssid_name_wt
),

--每个ssid的经纬度、与小区距离、与写字楼距离数据
--并对离住宅很近(<20米），离公司>住宅的ssid+经纬度，若名称不像公司则去除，若名称像公司则设定降权系数0.5
ssid_location_nameWt_info as
(
    select ssid,lat,lon,
           case 
             when min_home_dist<20 and (min_office_dist>min_home_dist or min_office_dist is null) then 0.5
             else ssid_name_wt end as ssid_name_wt
    from 
    (
        select ssid_location.ssid,ssid_location.lat,ssid_location.lon,ssid_location.ssid_name_wt,
               home.min_home_dist,work.min_office_dist
        from 
        (
            select ssid,lat,lon,ssid_name_wt
            from $tmp_work_ssid_location_wt
            where day = '$day'
        ) ssid_location
        left join ssid_home_distance_info home
        on ssid_location.ssid = home.ssid and ssid_location.lat = home.lat and ssid_location.lon = home.lon
        left join ssid_work_distance_info work
        on ssid_location.ssid = work.ssid and ssid_location.lat = work.lat and ssid_location.lon = work.lon
    )a
    where ssid_name_wt = 1
    or min_home_dist is null
    or min_home_dist > 20
    or min_office_dist <= min_home_dist
)

--补充ssid+lat+lon对应的bssid，并记录经纬度保留三位小数
insert overwrite table $tmp_work_ssid_bssid_location_info partition (day = '$day')
select x.ssid,y.bssid,x.lat,x.lon,round(x.lat,3) as lat_round,round(x.lon,3) as lon_round,x.ssid_name_wt
from ssid_location_nameWt_info x
inner join
(
    select bssid,ssid,lat,lon
    from $dim_mapping_bssid_location_mf
    where $bssidMappingLastParStr 
    and bssid_type = 1 
)y
on x.ssid = y.ssid and x.lat = y.lat and x.lon = y.lon;
"

deviceLocation3MPartition=`hive -e "show partitions $rp_device_location_3monthly" | sort| tail -n 1`
hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_distance as 'com.youzu.mob.java.udf.WGS84Distance';

--设备在工作日白天连接ssid+lat+lon的天数，计算距离用
--计算三个月内每个工作类ssid的每个在工作日白天连接过的device的连接天数
with work_ssid_lat_lon_device_connectDays_info as 
(
    select device,ssid,lat as lat_ssid,lon as lon_ssid,lat_round,lon_round,ssid_name_wt
          ,count(1) as bssid_days
    from
    (
        select a.ssid,a.lat,a.lon,a.lat_round,a.lon_round,a.ssid_name_wt,b.device,b.real_date
        from
        (
            select bssid,ssid,lat,lon,lat_round,lon_round,ssid_name_wt
            from $tmp_work_ssid_bssid_location_info
            where day = '$day'
        )a
        inner join
        (
            select device,bssid,real_date 
            from $tmp_workday_bssid_device_info
            where day = '$day'
        ) b 
        on a.bssid = b.bssid
        group by a.ssid,a.lat,a.lon,a.lat_round,a.lon_round,a.ssid_name_wt,b.device,b.real_date
    )c 
    group by device,ssid,lat,lon,lat_round,lon_round,ssid_name_wt
),


--将经纬度聚合到3位小数，计算设备在工作日白天连接ssid+lat_round+lon_round的天数，计算权重用
--连接2天或以上的设备作为常连设备
--ssid+lat_round+lon_round的常连设备数大于1,小于10000
work_ssid_latRound_lonRound_device_connectDays_info as 
(
    select device,ssid,lat_round,lon_round,ssid_name_wt,bssid_days,freq_dev_num
    from 
    (
        select device,ssid,lat_round,lon_round,ssid_name_wt,bssid_days,
               count(1) over(partition by ssid,lat_round,lon_round) as freq_dev_num
        from
        (
            select device,ssid,lat_round,lon_round,ssid_name_wt,count(1) as bssid_days
            from
            (
                select a.ssid,a.lat_round,a.lon_round,a.ssid_name_wt,b.device,b.real_date
                from
                (
                    select bssid,ssid,lat_round,lon_round,ssid_name_wt
                    from $tmp_work_ssid_bssid_location_info
                    where day = '$day'
                )a
                inner join
                (
                    select device,bssid,real_date 
                    from $tmp_workday_bssid_device_info
                    where day = '$day'
                ) b 
                on a.bssid = b.bssid
                group by a.ssid,a.lat_round,a.lon_round,a.ssid_name_wt,b.device,b.real_date
            )c 
            group by device,ssid,lat_round,lon_round,ssid_name_wt
            having count(1) >= 2
        )d
    )e
    where freq_dev_num > 1 
    and freq_dev_num < 10000
)

insert overwrite table $tmp_work_ssid_device_distance_confidence_info partition (day = '$day')
select x1.device,
       x1.ssid,
       x1.lat_round,
       x1.lon_round,
       if(x2.device is not null,x2.work_dist,99999) as distance,
       if(x2.device is not null,confidence_work,0) as confidence,
       x1.bssid_days,
       x1.ssid_name_wt
from 
work_ssid_latRound_lonRound_device_connectDays_info x1
left join
(
    select b.device,b.ssid,b.lat_round,b.lon_round,a.confidence_work,
           min(get_distance(a.lat_work,a.lon_work,b.lat_ssid,b.lon_ssid)) as work_dist
    from
    (   
        select x.device,y.lat_work,y.lon_work,if(y.confidence_work > 0,y.confidence_work,0) as confidence_work
        from
        (
            select device 
            from work_ssid_latRound_lonRound_device_connectDays_info
            group by device
        )x
        inner join
        (
            select device,lat_work,lon_work,confidence_work
            from $rp_device_location_3monthly
            where $deviceLocation3MPartition 
            and lat_work > 0 
            and lon_work > 0 
            and confidence_work > 0
        )y 
        on x.device = y.device
    )a
    inner join work_ssid_lat_lon_device_connectDays_info b
    on a.device = b.device
    group by b.device,b.ssid,b.lat_round,b.lon_round,a.confidence_work
)x2
on x1.device=x2.device and x1.ssid=x2.ssid and x1.lat_round=x2.lat_round and x1.lon_round=x2.lon_round;
"

#计算得出关系表
hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;

--每个设备在工作日白天出现的天数，作为工作关系置信度的分母
with work_device_activeDays_info as 
(
    select device,count(1) as day_num
    from
    (
        select device,real_date 
        from $tmp_workday_bssid_device_info
        where day = '$day'
        group by device,real_date
    )y 
    group by device
),

---根据工作地、bssid连接次数、设备活跃天数，给出bssid与device得分
work_device_ssid_score_info as 
(
    select ssid,
           device,
           lat_round,
           lon_round,
           if(conf_work>0,1,0) as if_bssid_work,
           sqrt((conf_work+bssid_days/day_num)/2*ssid_name_wt) as bssid_device_score
    from
    (
        select x.ssid,
               x.device,
               x.lat_round,
               x.lon_round,
               if(x.distance<= 100,x.confidence,0) as conf_work,
               x.bssid_days,
               x.ssid_name_wt,
               y.day_num
        from 
        (
            select ssid,device,lat_round,lon_round,confidence,distance,bssid_days,ssid_name_wt
            from $tmp_work_ssid_device_distance_confidence_info
            where day = '$day'
        ) x
        inner join work_device_activeDays_info y
        on x.device = y.device
    )a
),

--对于属于同个bssid的不同设备，
--若至少有一方为工作地，则计算得分
work_ssid_deviceTodevice_score_info as 
(
    select  x.device,
            y.device as device_work,
            x.bssid_device_score*y.bssid_device_score as device_device_score,
            x.if_bssid_work+y.if_bssid_work as work_type
    from 
    work_device_ssid_score_info x
    inner join work_device_ssid_score_info y
    on x.ssid = y.ssid and x.lat_round = y.lat_round and x.lon_round = y.lon_round
    where x.device != y.device
)

insert overwrite table $device_work_relation_info partition (day = '$day')
select device,
       device_work,
       round(if(max(device_device_score)<1,1 - power(2,sum(log2(1-device_device_score))),1),2) as confidence,
       max(work_type) as work_type
from work_ssid_deviceTodevice_score_info
group by device,device_work;
"