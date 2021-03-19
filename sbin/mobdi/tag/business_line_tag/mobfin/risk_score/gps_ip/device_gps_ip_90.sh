#!/bin/bash
: '
@owner:luost
@describe:近90天gps
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p90day=`date -d "$day -90 days" +%Y%m%d`


#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di

#mapping表
#geohash6_area_mapping_par=dim_sdk_mapping.geohash6_area_mapping_par
#geohash8_lbs_info_mapping_par=dim_sdk_mapping.geohash8_lbs_info_mapping_par
#mapping_ip_attribute_code=dim_sdk_mapping.mapping_ip_attribute_code

tmp_anticheat_device_gps_ip_location=dw_mobdi_tmp.tmp_anticheat_device_gps_ip_location

#out
#label_l1_anticheat_device_riskscore=dm_mobdi_report.label_l1_anticheat_device_riskscore

ipmappingPartition=`hive -S -e "show partitions $mapping_ip_attribute_code" | sort |tail -n 1`

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coord_convertor as 'com.youzu.mob.java.udf.CoordConvertor';

create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';

with tmp_anticheat_device_gps_ip_pre as (
    select muid as device,
           split(coord_convertor(latitude,longitude,'wsg84','bd09'), ',')[0] as lat, 
           split(coord_convertor(latitude,longitude,'wsg84','bd09'), ',')[1] as lon, 
           clientip
    from $dwd_location_info_sec_di
    where day > '$p90day' 
    and day <= '$day'
    and abs(latitude) <= 90 
    and abs(longitude) <= 180 
    and (latitude <> 0 or longitude <> 0) 
    and clientip is not null 
    and clientip <> ''
    and latitude is not null 
    and longitude is not null
    and serdatetime is not null 
    and clienttime is not null
    and (serdatetime - clienttime) <= 60000
    and (latitude - round(latitude, 1))*10 <> 0.0 
    and (longitude - round(longitude, 1))*10 <> 0.0
),

gps_ip_vs_pre as(
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
        from tmp_anticheat_device_gps_ip_pre
        where abs(lat) < 90.1
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

insert overwrite table $tmp_anticheat_device_gps_ip_location partition(day = '$day',timewindow = '90')
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

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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

with device_gps_ip_pre as (
    select device, sum(cnt) as cnt_all, sum(cnt_effective) as cnt_effective_all
    from 
    (
        select device,cnt,help,weigth,cnt*weigth as cnt_effective
        from 
        (
            select device,cnt,help, 
            case 
              when help = 3 then 1
              when help = 2 then 0.5
              when help = 1 then 0.2
            else 0
            end as weigth
            from 
            (
                select device,cnt,(country_help + province_help + city_help) as help
                from 
                (
                    select device,cnt, 
                    case 
                      when country_code_ip = country_code_gps then 1
                    else 0
                    end as country_help,
                    case 
                      when province_code_ip = province_code_gps then 1
                    else 0
                    end as province_help,
                    case 
                      when city_code_ip = city_code_gps then 1
                    else 0
                    end as city_help
                    from $tmp_anticheat_device_gps_ip_location
                    where day = '$day'
                    and timewindow = '90'
                ) a 
            ) b 
        ) c 
    ) d 
    group by device
)

insert overwrite table $label_l1_anticheat_device_riskscore partition (day = '$day',timewindow = '90',flag = '9')
select device,1 - (cnt_effective_all/cnt_all) as riskScore
from device_gps_ip_pre;
"