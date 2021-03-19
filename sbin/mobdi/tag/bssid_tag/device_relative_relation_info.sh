#!/bin/bash
: '
@owner:luost
@describe:亲友共网关系
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
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#源表
#dwd_log_wifi_info_sec_di=dm_mobdi_master.dwd_log_wifi_info_sec_di
#rp_device_location_3monthly=dm_mobdi_report.rp_device_location_3monthly

#mapping表
#dim_mapping_bssid_location_mf=dm_mobdi_mapping.dim_mapping_bssid_location_mf
#dim_traffic_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_traffic_ssid_bssid_match_info_mf
#dim_shopping_mall_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_shopping_mall_ssid_bssid_match_info_mf
#dim_hotel_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_hotel_ssid_bssid_match_info_mf
#dim_car_4s_ssid_bssid_match_info_mf=dm_mobdi_mapping.dim_car_4s_ssid_bssid_match_info_mf

#中间表
tmp_home_geohash_info=dm_mobdi_tmp.tmp_home_geohash_info
tmp_work_geohash_info=dm_mobdi_tmp.tmp_work_geohash_info
tmp_relatives_friends_bssid_ssid_location_info=dm_mobdi_tmp.tmp_relatives_friends_bssid_ssid_location_info
tmp_relatives_friends_bssid_ssid_etlIdentifiedBssid_info=dm_mobdi_tmp.tmp_relatives_friends_bssid_ssid_etlIdentifiedBssid_info
tmp_relatives_friends_bssid_ssid_clean_info=dm_mobdi_tmp.tmp_relatives_friends_bssid_ssid_clean_info
tmp_relatives_friends_bssid_device_connectDays_dinstance_info=dm_mobdi_tmp.tmp_relatives_friends_bssid_device_connectDays_dinstance_info

#输出表
#device_relatives_friends_relation_info_mi=dm_mobdi_report.device_relatives_friends_relation_info_mi

#亲友一度关系中间表建立
hive -v -e "
create table if not exists $tmp_relatives_friends_bssid_ssid_location_info(
    bssid string comment 'bssid',
    ssid string comment 'ssid',
    lat string comment '纬度',
    lon string comment '经度'
)
comment '亲友一度关系，bssid位置信息中间表'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_relatives_friends_bssid_ssid_etlIdentifiedBssid_info(
    bssid string comment 'bssid',
    ssid string comment 'ssid',
    lat string comment '纬度',
    lon string comment '经度'
)
comment '亲友一度关系，清洗公共场所和已识别bssid后的中间表'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_relatives_friends_bssid_ssid_clean_info(
    bssid string comment 'bssid',
    ssid string comment 'ssid',
    lat string comment '纬度',
    lon string comment '经度'
)
comment '亲友一度关系，家庭类bssid信息中间表'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_relatives_friends_bssid_device_connectDays_dinstance_info(
    bssid string comment 'bssid',
    device string comment '设备号',
    connect_days string comment '连接天数',
    confidence string comment '置信度',
    distance string comment 'bssid和device居住地距离'
)
comment '亲友一度关系，家庭类bssid的设备连接信息中间表'
partitioned by (day string comment '日期')
stored as orc;
"

#输出表建立
hive -v -e "
create table if not exists $device_relatives_friends_relation_info_mi(
    device string comment '设备号',
    device_relative string comment '亲友关系设备',
    confidence double comment '置信度，0-1，越大关系越强',
    relative_type string comment '关系类型（0都不是家庭，可能是朋友；1有一方家庭，可能是亲戚；2双方家庭，可能是家人/邻居）'
)
comment '一度亲友共网关系'
partitioned by (day string comment '日期')
stored as orc;
"

: '
@part_1:
实现功能:生成一度亲友关系设备网络信息
基本逻辑:
        1.筛选亲友/家庭类的bssid。
        2.找出连接这些bssid的设备，根据连接天数和位置信息，计算bssid与设备之间的权重。
        3.对连接同一bssid的设备计算设备两两之间的权重。最后对边聚合权重。
        4.详细逻辑:http://c.mob.com/pages/viewpage.action?pageId=32316980
'

bssidMappingLastParStr=`hive -e "show partitions $dim_mapping_bssid_location_mf" | sort| tail -n 1`

#取三个月的dwd_log_wifi_info_sec_di数据，计算获得连接设备数为2-9的bssid数据
#并与dim_mapping_bssid_location_mf通过bssid关联获得对应的ssid,lat,lon
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

insert overwrite table $tmp_relatives_friends_bssid_ssid_location_info partition (day = '$day')
select trim(bssid_devNum_table.bssid) as bssid,trim(ssid) as ssid,lat,lon
from 
(
    select bssid,count(1) as dev_num
    from
    (
        select bssid,device
        from
        (
            select device,bssid,ssid,day,
            from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
            from $dwd_log_wifi_info_sec_di
            where day > '$p3monthDay' 
            and day <= '$day' 
            and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') 
            and length(bssid) > 10
        )x 
        where real_date > '$p3monthDay' 
        and real_date <= '$day'
        group by device,bssid
    )y 
    group by bssid
    having count(1) >= 2 and count(1) <= 9
) bssid_devNum_table
inner join
(
    select bssid,ssid,lat,lon
    from $dim_mapping_bssid_location_mf
    where $bssidMappingLastParStr 
    and bssid_type = 1
    and length(bssid) > 10
    and lat is not null
    and lon is not null
) location_table
on bssid_devNum_table.bssid = location_table.bssid
group by bssid_devNum_table.bssid,dev_num,ssid,lat,lon;
"

trafficParition=`hive -e "show partitions $dim_traffic_ssid_bssid_match_info_mf" | sort| tail -n 1`
shoppingPartition=`hive -e "show partitions $dim_shopping_mall_ssid_bssid_match_info_mf" | sort| tail -n 1`
hotelPartition=`hive -e "show partitions $dim_hotel_ssid_bssid_match_info_mf" | sort| tail -n 1`
carPartition=`hive -e "show partitions $dim_car_4s_ssid_bssid_match_info_mf" | sort| tail -n 1`
#清洗bssid
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

--过滤ssid明显为公司或者公共场所的数据
--并取同经纬度下ssid对应的bssid数量为1或2的数据
with relatives_friends_bssid_ssid_etlPublic_inf as
(
    select bssid,ssid,lat,lon
    from
    (
        select bssid,ssid,lat,lon,count(1) over(partition by ssid,lat,lon) as num
        from $tmp_relatives_friends_bssid_ssid_location_info
        where day = '$day'
        and lower(ssid) not rlike '.*hotel|酒店|bank|医院|免费|free|i-|地铁|edu|homeinns|hrxj|huazhu|餐厅|耀湘财富|huawei|七天|八大处|便利店|美丽人间|保险|市场部|宴会厅|健身|药房|美术|俱乐部|农庄|建材|office|红公馆|guest|访客.*'
        and lower(ssid) not rlike '.*中心|大厦|火锅|xiaomi|unknown ssid|wanda|chinalife|共享|pingan|行车记录|政府|小吃|物业|营业厅|student|teacher.*'
        and lower(ssid) not rlike '.*公园|广场|ktv|办公室|中国移动|乐园|中国电信|中国联通|oppo|欢迎|redmi|vivo|幼儿园|小学|中学|netcore|酒楼|iphone|小米|超市.*'
        and lower(ssid) not rlike '.*wifi|wlan'
    ) a
    where num <=2
)

--过滤已识别(交通、商场、酒店、4s店)的bssid
insert overwrite table $tmp_relatives_friends_bssid_ssid_etlIdentifiedBssid_info partition (day = $day)
select rf.bssid,ssid,lat,lon
from 
relatives_friends_bssid_ssid_etlPublic_inf rf
left join
(
    select bssid
    from $dim_traffic_ssid_bssid_match_info_mf
    lateral view explode(bssid_array) n as bssid
    where $trafficParition
) traffic
on rf.bssid = traffic.bssid
left join
(
    select bssid
    from $dim_shopping_mall_ssid_bssid_match_info_mf
    lateral view explode(bssid_array) n as bssid
    where $shoppingPartition
) shopping_mall
on rf.bssid = shopping_mall.bssid
left join
(
    select bssid
    from $dim_hotel_ssid_bssid_match_info_mf
    lateral view explode(bssid_array) n as bssid
    where $hotelPartition
) hotel
on rf.bssid = hotel.bssid
left join
(
    select bssid
    from $dim_car_4s_ssid_bssid_match_info_mf
    lateral view explode(bssid_array) n as bssid
    where $carPartition
) car_4s
on rf.bssid = car_4s.bssid
where traffic.bssid is null
and shopping_mall.bssid is null
and hotel.bssid is null
and car_4s.bssid is null;
"

#去除离工作地很近（<20米），离小区不近（距离最近小区>最近工作地）的bssid
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

--计算连接数为2-9的bssid与匹配到的小区之间的距离
with bssid_home_distance_info as 
(
    select x.bssid,
           min(get_distance(x.lat,x.lon,y.lat,y.lon)) as min_bssid_home_dist
    from
    (
        select bssid,ssid,lat,lon,get_geohash(lat,lon,6) as bssid_geohash6
        from $tmp_relatives_friends_bssid_ssid_location_info
        where day = '$day'
        and lower(ssid) not rlike '.*免费|free|i-|地铁|plaza|智慧城市|服务区|unknown ssid|热线.*'  
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
    group by x.bssid
),

--计算连接数为2-9的bssid与匹配到的工作地之间的距离
bssid_work_distance_info as 
(
    select x.bssid,
           min(get_distance(x.lat,x.lon,y.lat,y.lon)) as min_bssid_office_dist
    from
    (
        select bssid,ssid,lat,lon,get_geohash(lat,lon,6) as bssid_geohash6
        from $tmp_relatives_friends_bssid_ssid_location_info
        where day = '$day'
        and lower(ssid) not rlike '.*免费|free|i-|地铁|plaza|智慧城市|服务区|unknown ssid|热线.*'  
    )x
    inner join
    (
        select lat,lon,geohash6
        from $tmp_work_geohash_info
        where day = '$day'
        and lat > 0 
        and lon > 0 
    )y
    on x.bssid_geohash6 = y.geohash6
    group by x.bssid
),

--匹配出离工作地近(<20m)、离小区不近(距离最近小区距离>距离最近工作地距离)的bssid
bssid_etlHomeGtWork_info as 
(
    select bssid_info.bssid
    from 
    (
        select bssid,get_geohash(lat,lon,6) as bssid_geohash6
        from $tmp_relatives_friends_bssid_ssid_location_info
        where day = '$day'
        and lower(ssid) not rlike '.*免费|free|i-|地铁|plaza|智慧城市|服务区|unknown ssid|热线.*'
    ) bssid_info
    left join bssid_home_distance_info home
    on bssid_info.bssid = home.bssid
    left join bssid_work_distance_info work
    on bssid_info.bssid = work.bssid
    where home.min_bssid_home_dist < 20
    and (work.min_bssid_office_dist < home.min_bssid_home_dist or home.min_bssid_home_dist is null)
)

--再关联清理了已识别和公共的bssid信息得到最终清洗的家庭类bssid数据
insert overwrite table $tmp_relatives_friends_bssid_ssid_clean_info partition (day = '$day')
select x.bssid,x.ssid,x.lat,x.lon
from 
(
    select bssid,ssid,lat,lon
    from $tmp_relatives_friends_bssid_ssid_etlIdentifiedBssid_info
    where day = '$day'
) x
left join bssid_etlHomeGtWork_info y
on x.bssid = y.bssid
where y.bssid is null;
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

--三个月内出现过的device信息 inner join 清理后得到的家庭类bssid信息
--按照bssid,lat,lon,device,real_date去重得到每个家庭类bssid对应连接的每个device的不重复连接日期
--再按照bssid,lat,lon,device去重得到三个月内每个家庭类bssid对应的每个device和连接天数
--再过滤出三个月内每个家庭类bssid对应的每个连接天数>=2的device信息
--再过滤出三个月内有两个及以上的连接天数>=2的device对应的每个家庭类bssid
--输出数据:有2个及以上连接天数超过2天的设备对应的家庭类bssid和其连接的device、连接天数
with bssid_device_connectDays_info as (
    select bssid,device,lat_bssid,lon_bssid,bssid_days
    from
    (
        select bssid,device,lat_bssid,lon_bssid,bssid_days,count(1) over(partition by bssid) as freq_dev_num
        from 
        (
            select bssid,device,lat as lat_bssid,lon as lon_bssid,count(1) as bssid_days
            from
            (
                select a.bssid,a.lat,a.lon,b.device,b.real_date
                from
                (
                    select bssid,ssid,lat,lon
                    from $tmp_relatives_friends_bssid_ssid_clean_info
                    where day = '$day'
                )a
                inner join
                (
                    select device,bssid,real_date 
                    from
                    ( 
                        select device,bssid,ssid,
                               from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
                        from $dwd_log_wifi_info_sec_di
                        where day > '$p3monthDay' 
                        and day <= '$day' 
                        and bssid is not null 
                        and length(trim(bssid)) > 2
                    )x 
                    where real_date > '$p3monthDay' 
                    and real_date <= '$day'
                ) b 
                on a.bssid = b.bssid
                group by a.bssid,a.lat,a.lon,b.device,b.real_date
            )c 
            group by device,bssid,lat,lon
            having count(1) >=2 
        )d
    )e
    where freq_dev_num > 1
)

--得到每个家庭类bssid对应连接的device的居住地位置信息
--并计算每个家庭类bssid和其对应连接的每个device居住地的距离
--输出数据:有2个及以上连接天数超过2天的设备对应的家庭类bssid和其连接的device、连接天数、bssid和device居住地距离、置信度
insert overwrite table $tmp_relatives_friends_bssid_device_connectDays_dinstance_info partition (day = '$day')
select bssid,device,bssid_days as connect_days,confidence_home as confidence,
       if(
          confidence_home>0 and lat_home>0 and lat_bssid>0,
          get_distance(lat_home,lon_home,lat_bssid,lon_bssid),
          999999
          ) as distance
from
(
    select x.bssid,x.device,x.lat_bssid,x.lon_bssid,x.bssid_days,
           y.lat_home,y.lon_home,
           if(y.confidence_home>0,y.confidence_home,0) as confidence_home
    from bssid_device_connectDays_info x
    left join
    (
        select device,lat_home,lon_home,confidence_home
        from $rp_device_location_3monthly
        where $deviceLocation3MPartition 
        and lat_home is not null
    )y
    on x.device = y.device
)a;
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

--三个月内每个设备出现的天数，作为亲友关系置信度的分母
with device_activeDays_info as (
    select device,count(1) as day_num
    from
    (
        select device,real_date
        from
        (
            select device,bssid,ssid,day
                  ,from_unixtime(cast(substring(datetime, 1, 10) as bigint), 'yyyyMMdd') as real_date
            from $dwd_log_wifi_info_sec_di
            where day > '$p3monthDay' 
            and day <= '$day' 
            and regexp_replace(lower(trim(bssid)), ':|-|\\\\.|\073', '') not in ('000000000000', '020000000000', 'ffffffffffff') 
            and bssid is not null 
        )x 
        where real_date > '$p3monthDay' 
        and real_date <= '$day'
        group by device,real_date
    )y 
    group by device
),

--计算设备与该bssid的连接得分 X_a = sqrt(( device连接bssid天数/device活跃天数+ 居住地置信度 )/2)
bssid_device_connectScore_info as 
(
    select bssid,
           device,
           if(conf_home>0,1,0) as if_bssid_home,
           sqrt((conf_home+connect_days/day_num)/2) as bssid_device_score
    from
    (
       select x.bssid,x.device,x.connect_days,
              if(x.distance <= 100,x.confidence,0) as conf_home,
              y.day_num
       from 
       (
           select bssid,device,connect_days,confidence,distance
           from $tmp_relatives_friends_bssid_device_connectDays_dinstance_info
           where day = '$day' 
       ) x
       inner join device_activeDays_info y
       on x.device = y.device
    )a
),

bssid_deviceTodevice_relation_info as 
(
    select  x.device,
            y.device as device_relative,
            x.bssid_device_score*y.bssid_device_score as device_device_score,
            x.if_bssid_home+y.if_bssid_home as relative_type
    from bssid_device_connectScore_info x
    join bssid_device_connectScore_info y
    on x.bssid = y.bssid
    where x.device != y.device
)

--最终输出
insert overwrite table $device_relatives_friends_relation_info_mi partition (day = '$day')
select device,
       device_relative,
       round(if(max(device_device_score)<1, 1 - power(2,sum(log2(1-device_device_score))),1),2) as confidence,
       max(relative_type) as relative_type
from bssid_deviceTodevice_relation_info
group by device,device_relative;
"