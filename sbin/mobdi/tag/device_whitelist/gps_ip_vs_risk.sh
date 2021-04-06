#!/bin/sh

set -e -x

day=$1

p1months=`date -d "$day -30 days" +%Y%m`16
p2months=`date -d "$day -60 days" +%Y%m`16
p3months=`date -d "$day -90 days" +%Y%m`16

source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties

#input
#dwd_location_info_sec_di=dm_mobdi_master.dwd_location_info_sec_di

#mapping
#dim_geohash6_china_area_mapping_par=dim_sdk_mapping.dim_geohash6_china_area_mapping_par
#dim_geohash8_china_area_mapping_par=dim_sdk_mapping.dim_geohash8_china_area_mapping_par
#dim_mapping_ip_attribute_code=dim_sdk_mapping.dim_mapping_ip_attribute_code

#md/output
gps_ip_info_incr_pre=${dm_mobdi_tmp}.gps_ip_info_incr_pre
gps_ip_risk_pre=${dm_mobdi_tmp}.gps_ip_risk_pre
device_gps_ip_risk=${dm_mobdi_tmp}.device_gps_ip_risk

hive -e"
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
SET mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';

add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coord_convertor as 'com.youzu.mob.java.udf.CoordConvertor';

insert overwrite table $gps_ip_info_incr_pre
select a.muid as device, split(coord_convertor(latitude,longitude,'wsg84','bd09'), ',')[0] as lat, split(coord_convertor(latitude,longitude,'wsg84','bd09'), ',')[1] as lon, clientip,day
    from 
    (select * from $dwd_location_info_sec_di
     where day >= '$p3months' and day <= '$day'
    and abs(latitude) <= 90 and abs(longitude) <= 180 and (latitude <> 0 or longitude <> 0) and clientip is not null and clientip <> ''
    and latitude is not null and longitude is not null
    and serdatetime is not null and clienttime is not null
    and (serdatetime - clienttime) <= 60000
    and ((latitude - round(latitude, 1))*10 <> 0.0 and (longitude - round(longitude, 1))*10 <> 0.0)
     )a
    left semi join
    ( 
     select muid from $dwd_location_info_sec_di
     where day between '$p1months' and '$day' 
     and abs(latitude) <= 90 and abs(longitude) <= 180 and (latitude <> 0 or longitude <> 0) 
     and clientip is not null and clientip <> '' and latitude is not null and longitude is not null
     and serdatetime is not null and clienttime is not null
     and (serdatetime - clienttime) <= 60000
     and ((latitude - round(latitude, 1))*10 <> 0.0 and (longitude - round(longitude, 1))*10 <> 0.0)
    group by muid
    ) b
 on a.muid=b.muid
"


hive -e"

SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
SET mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;


add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coord_convertor as 'com.youzu.mob.java.udf.CoordConvertor';
create temporary function get_geohash as 'com.youzu.mob.java.udf.GetGeoHash';
create temporary function get_ip_attribute as 'com.youzu.mob.java.udf.GetIpAttribute';


with gps_ip_vs_pre_1month as(
  select device, get_geohash(lat, lon, 8) as geohash8, clientip, count(*) as cnt
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
    end as lon, clientip
    from 
    $gps_ip_info_incr_pre
    where day between '$p1months' and '$day' and abs(lat) < 90.1 and abs(lon) < 180.1
  ) as d
  group by device, get_geohash(lat, lon, 8), clientip
),
gps_location_1month as(
select device, clientip, province_code_gps, city_code_gps, sum(cnt) as cnt
from 
(
  select gps_ip_vs_pre_1month1.device, gps_ip_vs_pre_1month1.geohash8, gps_ip_vs_pre_1month1.clientip, gps_ip_vs_pre_1month1.cnt, geohash6_mapping1.province_code as province_code_gps, geohash6_mapping1.city_code as city_code_gps
  from gps_ip_vs_pre_1month gps_ip_vs_pre_1month1
  left join 
  (
    select geohash_6_code, province_code, city_code
    from $dim_geohash6_china_area_mapping_par
    where version = '1000'
  )as geohash6_mapping1
  on substring(gps_ip_vs_pre_1month1.geohash8, 1, 6) = geohash6_mapping1.geohash_6_code
  where geohash6_mapping1.geohash_6_code is not null 
  
  union all 
  
  select t.device, t.geohash8, t.clientip, t.cnt, geohash8_mapping.province_code as province_code_gps, geohash8_mapping.city_code as city_code_gps
  from 
  (
    select gps_ip_vs_pre_1month2.*
    from gps_ip_vs_pre_1month  gps_ip_vs_pre_1month2
    left join 
    (
      select geohash_6_code, province_code, city_code
      from $dim_geohash6_china_area_mapping_par
      where version = '1000'
    )as geohash6_mapping2 
    on substring(gps_ip_vs_pre_1month2.geohash8, 1, 6) = geohash6_mapping2.geohash_6_code
    where geohash6_mapping2.geohash_6_code is null 
  ) t 
  left join 
  (
    select geohash_8_code, province_code, city_code
    from $dim_geohash8_china_area_mapping_par
    where version = '1000'
  ) as geohash8_mapping 
  on t.geohash8 = geohash8_mapping.geohash_8_code
) as p 
group by device, clientip, province_code_gps, city_code_gps
),
ip_location_1month as(
select a.device, a.clientip, mapping_ip_attribute.country, mapping_ip_attribute.province, mapping_ip_attribute.city, mapping_ip_attribute.country_code as country_code_ip, mapping_ip_attribute.province_code as province_code_ip, mapping_ip_attribute.city_code as city_code_ip
from 
(
  select device, clientip, get_ip_attribute(clientip) as minip
  from 
  (
    select device, clientip
    from gps_location_1month
    group by device, clientip
  ) as gps_location 
) as a 
left join 
(
  select minip, country, province, city, country_code, province_code, city_code
  from $dim_mapping_ip_attribute_code
  where day = '20190104'
) as mapping_ip_attribute 
on a.minip = mapping_ip_attribute.minip
),
gpslocation_iplocation_1month as(
  select ip_location_1month.device, ip_location_1month.clientip, ip_location_1month.country, ip_location_1month.province, ip_location_1month.city, ip_location_1month.country_code_ip, ip_location_1month.province_code_ip, ip_location_1month.city_code_ip, gps_location_1month.country_code_gps, gps_location_1month.province_code_gps, gps_location_1month.city_code_gps, gps_location_1month.cnt
  from 
  (
    select device, clientip, country, province, city, country_code_ip, province_code_ip, city_code_ip
    from ip_location_1month
  ) as ip_location_1month 
  left join 
  (
    select device, clientip, 
    case 
      when province_code_gps is null or province_code_gps = '' then 'other'
    else 'cn'
    end as country_code_gps, province_code_gps, city_code_gps, cnt
    from gps_location_1month
) as gps_location_1month 
on ip_location_1month.device = gps_location_1month.device and ip_location_1month.clientip = gps_location_1month.clientip
)
insert overwrite  table $gps_ip_risk_pre partition(day=$day)
select device, sum(cnt) as cnt_all, sum(cnt_effective) as cnt_effective_all
from 
(
  select device, cnt, help, weigth, cnt*weigth as cnt_effective
  from 
  (
    select device, cnt, help, 
    case 
      when help = 3 then 1
      when help = 2 then 0.5
      when help = 1 then 0.2
    else 0
    end as weigth
    from 
    (
      select device, cnt, (country_help + province_help + city_help) as help
      from 
      (
        select device, cnt, 
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
        from gpslocation_iplocation_1month
        where country_code_gps = 'cn'
      ) as a 
    ) as b 
  ) as c 
) as d 
group by device

"

hive -e"
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts='-Xmx3g';
SET mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts='-Xmx3g';
SET mapreduce.map.java.opts='-Xmx3g';
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

with gps_ip_vs_1month_risk as(
  select device, 1 - (cnt_effective_all/cnt_all) as risk
  from  $gps_ip_risk_pre
  where day =$day
),
gps_ip_vs_2month_risk as(
  select device, 1- (sum(cnt_effective_all)/sum(cnt_all)) as risk
  from 
  (
    select device, cnt_effective_all, cnt_all
    from $gps_ip_risk_pre
    where day=$day
    union all 
    select device, cnt_effective_all, cnt_all
    from $gps_ip_risk_pre
    where day=$p1months
  ) as a 
  group by device
),
gps_ip_vs_3month_risk as(
  select device, 1 - (sum(cnt_effective_all)/sum(cnt_all)) as risk
  from 
  (
    select device, cnt_effective_all, cnt_all
    from $gps_ip_risk_pre
    where day=$day
    union all 
    select device, cnt_effective_all, cnt_all
    from $gps_ip_risk_pre
    where day=$p1months
    union all 
    select device, cnt_effective_all, cnt_all
    from $gps_ip_risk_pre
    where day=$p2months
  ) as a 
  group by device
)
insert overwrite table $device_gps_ip_risk
select device, avg(risk) as gps_ip_vs_risk
from 
(
  select device, risk
  from gps_ip_vs_1month_risk
  union all 
  select device, risk
  from gps_ip_vs_2month_risk
  union all 
  select device, risk
  from gps_ip_vs_3month_risk
) as a 
group by device
"