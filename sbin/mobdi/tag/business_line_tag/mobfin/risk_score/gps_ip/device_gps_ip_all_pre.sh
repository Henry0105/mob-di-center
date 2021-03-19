#!/bin/bash
: '
@owner:luost
@describe:gps,ip相关标签预处理
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#源表
tmp_anticheat_device_gps_ip_pre=dw_mobdi_tmp.tmp_anticheat_device_gps_ip_pre

#mapping表
#geohash6_area_mapping_par=dim_sdk_mapping.geohash6_area_mapping_par
#geohash8_lbs_info_mapping_par=dim_sdk_mapping.geohash8_lbs_info_mapping_par
#mapping_ip_attribute_code=dim_sdk_mapping.mapping_ip_attribute_code

#输出表
tmp_anticheat_device_gps_ip_location=dw_mobdi_tmp.tmp_anticheat_device_gps_ip_location

ipmappingPartition=`hive -S -e "show partitions $mapping_ip_attribute_code" | sort |tail -n 1`

function device_gps_ip(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
SET hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfile =true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=200000000;

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';

with gps_ip_vs_pre as(
    select device,get_geohash(lat, lon, 8) as geohash8,clientip,count(1) as cnt
    from
    (
        select device,
        case
          when lat > 90 then 90
          when lat < -90 then -90
        else lat
        end as lat,
        case
          when lon > 180 then 180
          when lon < -180 then -180
        else lon
        end as lon,
        clientip
        from $tmp_anticheat_device_gps_ip_pre
        where day = '$day'
        and connect_day <= '$day'
        and connect_day > '$pday'
        and abs(lat) < 90.1
        and abs(lon) < 180.1
    ) as d
    group by device,get_geohash(lat, lon, 8),clientip
),

gps_location_pre as (
    select a.device,a.geohash8,a.clientip,a.cnt,
           b.province_code as province_code_gps,
           b.city_code as city_code_gps,
           b.geohash_6_code
    from gps_ip_vs_pre a
    left join
    (
        select geohash_6_code,province_code,city_code
        from $geohash6_area_mapping_par
        where version = '1000'
    ) b
    on substring(a.geohash8,1,6) = b.geohash_6_code
),

gps_location as(
    select device,clientip,province_code_gps,city_code_gps,sum(cnt) as cnt
    from
    (
        select device,geohash8,clientip,cnt,province_code_gps,city_code_gps
        from gps_location_pre
        where geohash_6_code is not null

        union all

        select t.device,t.geohash8,t.clientip,t.cnt,
               geohash8_mapping.province_code as province_code_gps,
               geohash8_mapping.city_code as city_code_gps
        from
        (
            select device,geohash8,clientip,cnt
            from gps_location_pre
            where geohash_6_code is null
        ) t
        left join
        (
            select geohash_8_code,province_code,city_code
            from $geohash8_lbs_info_mapping_par
            where version = '1000'
        ) geohash8_mapping
        on t.geohash8 = geohash8_mapping.geohash_8_code
    ) p
    group by device,clientip,province_code_gps,city_code_gps
),

ip_location as(
    select a.device,a.clientip,
          mapping_ip_attribute.country_code as country_code_ip,
          mapping_ip_attribute.province_code as province_code_ip,
          mapping_ip_attribute.city_code as city_code_ip
    from
    (
        select device,clientip,get_ip_attribute(clientip) as minip
        from
        (
            select device,clientip
            from gps_location
            group by device,clientip
        ) devie_ip
    ) a
    left join
    (
        select minip,country_code,province_code,city_code
        from $mapping_ip_attribute_code
        where $ipmappingPartition
    ) mapping_ip_attribute
    on a.minip = mapping_ip_attribute.minip
)

insert overwrite table $tmp_anticheat_device_gps_ip_location partition(day = '$day',timewindow = '$timewindow')
select a.device, 
       a.country_code_ip, 
       a.province_code_ip, 
       a.city_code_ip, 
       b.country_code_gps, 
       b.province_code_gps, 
       b.city_code_gps, 
       b.cnt
from ip_location a
left join 
(
    select device,clientip, 
    case 
      when province_code_gps is null or province_code_gps = '' then 'other'
    else 'cn'
    end as country_code_gps,
    province_code_gps,city_code_gps,cnt
    from gps_location
) b 
on a.device = b.device and a.clientip = b.clientip
where country_code_gps = 'cn';
"
}

for i in 7 14 30
do
    device_gps_ip $i
done