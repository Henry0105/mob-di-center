#!/bin/bash
set -x -e
insert_day=$1
day=${insert_day:0:6}01
premonthday=`date -d "$day -1 month" +%Y%m01`
# 每月8号从https://mall.ipplus360.com/infos/download 下载 账号： 17621201623 密码：123!@# 解压密码：www.ipplus360.com
# 解压出文件 IP_basic_single_WGS84_en.txt IP_basic_single_WGS84.txt 需求 http://j.mob.com/browse/MOBDI-3195?filter=-1

#input
ip_basic_single_wgs84=dw_mobdi_md.ip_basic_single_wgs84
ip_basic_single_wgs84_en=dw_mobdi_md.ip_basic_single_wgs84_en
#tmp
mapping_ip_attribute_code_1=dw_mobdi_md.mapping_ip_attribute_code_1
mapping_ip_attribute_code_2=dw_mobdi_md.mapping_ip_attribute_code_2
mapping_ip_attribute_code_en=dw_mobdi_md.mapping_ip_attribute_code_en
mapping_ip_attribute_code_3=dw_mobdi_md.mapping_ip_attribute_code_3
mapping_ip_attribute_code_4=dw_mobdi_md.mapping_ip_attribute_code_4
mapping_area_new_procn=dw_mobdi_md.mapping_area_new_procn
mapping_area_new_proen=dw_mobdi_md.mapping_area_new_proen
mapping_area_code_province_new_all=dw_mobdi_md.mapping_area_code_province_new_all
mapping_area_par_pre=dw_mobdi_md.mapping_area_par_pre
mapping_area_code_more=dw_mobdi_md.mapping_area_code_more
mapping_ip_attribute_code_tmp=dw_mobdi_md.mapping_ip_attribute_code_tmp
mapping_area_code_max=dw_mobdi_md.mapping_area_code_max
mapping_area_code_province_new=dw_mobdi_md.mapping_area_code_province_new
mapping_area_code_city_new=dw_mobdi_md.mapping_area_code_city_new
mapping_area_new=dw_mobdi_md.mapping_area_new

#mapping
mapping_area_par=dm_sdk_mapping.mapping_area_par
#output
mapping_area_par_final=mobdi_test.baron_mapping_area_par
mapping_ip_attribute=dm_sdk_mapping.mapping_ip_attribute
#mapping_ip_attribute_code=dm_sdk_mapping.mapping_ip_attribute_code
mapping_ip_attribute_code=mobdi_test.baron_mapping_ip_attribute_code

cd "`dirname $0`"
file=`find . -name "IP_basic_single_WGS84*" -print`
 if [[ -n $file ]];then
 ls -a IP_basic_single_WGS84*
 rm IP_basic_single_WGS84*
fi

wget -O IP_basic_single_WGS84_en.zip 'https://mall.ipplus360.com/download/file?downloadId=1SDAdFrbzFTYZFXRwGsIgCCs8RAvYuihOONT9hH6HWs9omWKETB2JU8E0JPRRnCe&fileName=IP_basic_single_WGS84_en_mysql' &
wget -O IP_basic_single_WGS84.zip 'https://mall.ipplus360.com/download/file?downloadId=1SDAdFrbzFTYZFXRwGsIgCCs8RAvYuihOONT9hH6HWs9omWKETB2JU8E0JPRRnCe&fileName=IP_basic_single_WGS84_mysql' &

wait

unzip -P 'www.ipplus360.com' IP_basic_single_WGS84_en.zip "IP_basic_single_WGS84_en.txt"
unzip -P 'www.ipplus360.com' IP_basic_single_WGS84.zip "IP_basic_single_WGS84.txt"

HADOOP_USER_NAME=dba hive -e "
 drop table if exists $ip_basic_single_wgs84;
 create table $ip_basic_single_wgs84
 (id string,minip string,maxip string,continent string,areacode string,adcode string,country string,
 province string,city string,district string,lngwgs string,latwgs string,radius string,accuracy string,
 owner string,isp string,asnumber string,source string,zipcode string,timezone string)
 row format delimited fields terminated BY '\t' lines terminated BY '\n' stored AS TEXTFILE
 tblproperties('skip.header.line.count'= '"1"');
 drop table if exists $ip_basic_single_wgs84_en;
 create table $ip_basic_single_wgs84_en
 (id string,minip string,maxip string,continent string,areacode string,adcode string,country string,
 province string,city string,district string,lngwgs string,latwgs string,radius string,accuracy string,
 owner string,isp string,asnumber string,source string,zipcode string,timezone string)
 row format delimited fields terminated BY '\t' lines terminated BY '\n' stored AS TEXTFILE
 tblproperties('skip.header.line.count'= '"1"');
 LOAD DATA LOCAL INPATH './IP_basic_single_WGS84.txt' OVERWRITE  INTO TABLE $ip_basic_single_wgs84;
 LOAD DATA LOCAL INPATH './IP_basic_single_WGS84_en.txt' OVERWRITE  INTO TABLE $ip_basic_single_wgs84_en;
 "

cd "`dirname $0`"
file=`find . -name "IP_basic_single_WGS84*" -print`
 if [[ -n $file ]];then
 ls -a IP_basic_single_WGS84*
 rm IP_basic_single_WGS84*
fi

flag="flag=$premonthday"
count=`hive -e "desc formatted $mapping_area_par partition($flag);"|grep 'numRows'|awk -F ' ' '{print $2}'`

#中文版转bd09 先存到dm_sdk_mapping.mapping_ip_attribute
HADOOP_USER_NAME=dba hive -e"
ADD jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function coordinate_converter as 'com.youzu.mob.java.udf.CoordinateConverter';
insert overwrite table $mapping_ip_attribute partition(day=$day)
select
id,minip,maxip,continent,areacode,adcode,country,province,city,district,
split( coordinate_converter(wgs_lon,wgs_lat,'wgs84','bd09'),',')[0] as bd_lon,
split( coordinate_converter(wgs_lon,wgs_lat,'wgs84','bd09'),',')[1] as bd_lat,
wgs_lat,wgs_lon,radius,'' as scene,accuracy,owner,isp,asnumber,zipcode,timezone
from
(
select
regexp_replace(id,'\"','') as id,
regexp_replace(minip,'\"','') as minip,
regexp_replace(maxip,'\"','') as maxip,
regexp_replace(continent,'\"','') as continent,
regexp_replace(areacode,'\"','') as areacode,
regexp_replace(adcode,'\"','') as adcode,
regexp_replace(country,'\"','') as country,
regexp_replace(province,'\"','') as province,
regexp_replace(city,'\"','') as city,
case when regexp_replace(country,'\"','')='中国' then regexp_replace(district,'\"','') else '' end as district,
regexp_replace(latwgs,'\"','') as wgs_lat,
regexp_replace(lngwgs,'\"','') as wgs_lon,
regexp_replace(radius,'\"','') as radius,
regexp_replace(accuracy,'\"','') as accuracy,
regexp_replace(owner,'\"','') as owner,
regexp_replace(isp,'\"','') as isp,
regexp_replace(asnumber,'\"','') as asnumber,
regexp_replace(zipcode,'\"','') as zipcode,
regexp_replace(timezone,'\"','') as timezone
from $ip_basic_single_wgs84
group by id,minip,maxip,continent,areacode,adcode,country,province,city,district,
latwgs,lngwgs,radius,accuracy,owner,isp,asnumber,zipcode,timezone
)a;

--匹配code $mapping_area_par

drop table $mapping_ip_attribute_code_1;
create table $mapping_ip_attribute_code_1 stored as orc as
select a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
'' as province_en,'' as city_en,b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,
a.wgs_lon,a.wgs_lat,a.radius,'' as scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
from (select * from $mapping_ip_attribute where day=$day) a
left join (select * from $mapping_area_par where $flag) b
on a.country=b.country_poi and a.province=b.province_poi and a.city=b.city_poi and a.district=b.area_poi
group by  a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,a.wgs_lon,a.wgs_lat,a.radius,
a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone;

drop table $mapping_ip_attribute_code_2;
create table $mapping_ip_attribute_code_2 stored as orc as
select a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
'' as province_en,'' as city_en,b.country_code,'' as province_code,'' as city_code,'' as area_code,
a.bd_lon,a.bd_lat,a.wgs_lon,a.wgs_lat,a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
from (
    select * from $mapping_ip_attribute_code_1
    where country_code is null and province='' and city='') a
left join (
    select distinct country_poi,country_code
    from (
        select distinct country_poi,country_code from $mapping_area_par where $flag
        union all
        select distinct country as country_poi,country_code from $mapping_area_par where $flag)t
    ) b
on a.country=b.country_poi
group by a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,b.country_code,
a.bd_lon,a.bd_lat,a.wgs_lon,a.wgs_lat,a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
;

drop table $mapping_ip_attribute_code_en;
create table $mapping_ip_attribute_code_en stored as orc as
select distinct c.id,c.minip,c.maxip,c.continent,c.areacode,c.adcode,c.country,c.province,c.city,c.district,
 regexp_replace(d.province,'\"','') as province_en,regexp_replace(d.city,'\"','') as city_en,
c.bd_lon,c.bd_lat,c.wgs_lon,c.wgs_lat,c.radius,c.scene,c.accuracy,c.owner,c.isp,c.asnumber,c.zipcode,c.timezone
from (
  select * from $mapping_ip_attribute_code_1
  where country_code is null and country!='' and province!='') c
left join $ip_basic_single_wgs84_en d
on c.minip=regexp_replace(d.minip,'\"','') and c.maxip=regexp_replace(d.maxip,'\"','');

drop table $mapping_ip_attribute_code_3;
create table $mapping_ip_attribute_code_3 stored as orc as
select a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
a.province_en,a.city_en,b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,
a.wgs_lon,a.wgs_lat,a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
from $mapping_ip_attribute_code_en a
left join (
    select * from $mapping_area_par where $flag) b
on a.country=b.country_poi and a.province=b.province_poi and trim(a.city_en)=b.city_poi
group by a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
a.province_en,a.city_en,b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,
a.wgs_lon,a.wgs_lat,a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
;

drop table $mapping_ip_attribute_code_4;
create table $mapping_ip_attribute_code_4 stored as orc as
select a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
a.province_en,a.city_en,b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,
a.wgs_lon,a.wgs_lat,a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
from (
    select * from $mapping_ip_attribute_code_3
    where country_code is null) a
left join (
    select * from $mapping_area_par where $flag) b
on a.country=b.country_poi and trim(a.province_en)=b.province_poi and trim(a.city_en)=b.city_poi
group by a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
a.province_en,a.city_en,b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,
a.wgs_lon,a.wgs_lat,a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
;

--最终写入 mapping_ip_attribute_code
drop table $mapping_ip_attribute_code_tmp;
create table $mapping_ip_attribute_code_tmp as
select * from $mapping_ip_attribute_code_1
where country_code is not null
union all
select * from $mapping_ip_attribute_code_2
union all
select * from $mapping_ip_attribute_code_3
where country_code is not null
union all
select * from $mapping_ip_attribute_code_4;

drop table $mapping_area_new;
create table $mapping_area_new stored as orc as
select country,province,city,province_en,city_en
from $mapping_ip_attribute_code_4
where country_code is null
group by country,province,city,province_en,city_en ;

drop table $mapping_area_new_procn;
create table $mapping_area_new_procn stored as orc as
select b.country_s_code,b.province_s_code,b.city_s_code,b.area_s_code,b.country,b.province,a.city,a.province_en,a.city_en,
b.country_code,b.province_code,'' as city_code,''as area_code,a.country as country_poi,a.province as province_poi,
'' as city_poi,'' as area_poi,b.provincial_capital,b.country_en,b.is_hot,b.continents,b.travel_area,'' as area
from(
    select country,province,province_en,city,city_en
    from $mapping_area_new) a
left join (
    select distinct country_s_code,province_s_code,city_s_code,area_s_code,country,province,country_poi,
    province_poi,country_code,province_code,provincial_capital,country_en,is_hot,continents,travel_area
    from(
        select distinct country_s_code,province_s_code,'' as city_s_code,'' as area_s_code,country,province,country_poi,
        province_poi,country_code,province_code,provincial_capital,country_en,is_hot,continents,travel_area
        from $mapping_area_par where $flag
        union all
        select distinct country_s_code,province_s_code,'' as city_s_code,'' as area_s_code,country,province,country_poi,
        province as province_poi,country_code,province_code,provincial_capital,country_en,is_hot,continents,travel_area
        from $mapping_area_par where $flag) t
    where province_poi not in ('萨格勒布县','莫斯科州','明斯克州','巴提奈区')
    )b
on a.country=b.country_poi and a.province=b.province_poi;

drop table $mapping_area_new_proen;
create table $mapping_area_new_proen stored as orc as
select b.country_s_code,b.province_s_code,b.city_s_code,b.area_s_code,b.country,b.province,a.city,a.province_en,a.city_en,
b.country_code,b.province_code,'' as city_code,''as area_code,a.country_poi,a.province_poi,'' as city_poi,'' as area_poi,
b.provincial_capital,b.country_en,b.is_hot,b.continents,b.travel_area,'' as area
from(
    select country_poi,province_poi,province_en,city,city_en
    from $mapping_area_new_procn
    where country_code is null) a
left join (
    select distinct country_s_code,province_s_code,city_s_code,area_s_code,country,province,country_poi,
    province_poi,country_code,province_code,provincial_capital,country_en,is_hot,continents,travel_area
    from(
        select distinct country_s_code,province_s_code,'' as city_s_code,'' as area_s_code,country,province,country_poi,
        province_poi,country_code,province_code,provincial_capital,country_en,is_hot,continents,travel_area
        from $mapping_area_par where $flag
        union all
        select distinct country_s_code,province_s_code,'' as city_s_code,'' as area_s_code,country,province,country_poi,
        province as province_poi,country_code,province_code,provincial_capital,country_en,is_hot,continents,travel_area
        from $mapping_area_par where $flag) t
    )b
on a.country_poi=b.country_poi and a.province_en=b.province_poi;

drop table $mapping_area_code_max;
create table $mapping_area_code_max stored as orc as
select *,max(cast(city_num as int)) over(partition by country,province) city_max,
max(cast(province_num as int)) over(partition by country) province_max
from(
select *, case when province_code rlike '[A-Za-z]{1,2}[0-9]{1,2}' then split(city_code,'\\_')[1] else split(city_code,'\\_')[2] end as city_num,
regexp_replace(province_code,'[A-Za-z_]{1,3}','') province_num
from $mapping_area_par where $flag) a
;


drop table $mapping_area_code_province_new;
create table $mapping_area_code_province_new stored as orc as
select b.country_s_code,b.province_s_code,b.city_s_code,b.area_s_code,b.country,a.province_poi as province,'' as city,
b.country_code,concat(b.country_code,'_',b.province_max+a.id) as province_code,'' as city_code,''as area_code,a.country_poi,
a.province_poi as province_poi,b.city_poi,b.area_poi,b.provincial_capital,b.country_en,b.is_hot,b.continents,b.travel_area,b.area
from (
    select row_number() over(partition by country_poi) id,country_poi,province_poi
    from (
          select distinct country_poi,province_poi
          from $mapping_area_new_proen
          where country_code is null and province_poi!='0' and country_poi!='中国')t ) a
left join (
    select distinct *  from $mapping_area_code_max
    where country!='' and province='' and city='') b
on a.country_poi=b.country_poi;

drop table $mapping_area_code_province_new_all;
create table $mapping_area_code_province_new_all stored as orc as
select c.*,d.city_max
from(
    select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city_en as city,country_code,province_code,city_code,area_code,country_poi,province_poi,city as city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
    from $mapping_area_new_procn where country_code is not null) c
left join (
    select distinct country_poi,province_poi,city_max
    from $mapping_area_code_max) d
on c.country_poi=d.country_poi and c.province_poi=d.province_poi
union all
select c.*,d.city_max
from(
    select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city_en as city,country_code,province_code,city_code,area_code,country_poi,province_poi,city as city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
    from $mapping_area_new_proen where country_code is not null) c
left join (
    select distinct country_poi,province_poi,city_max
    from $mapping_area_code_max) d
on c.country_poi=d.country_poi and c.province=d.province_poi
union all
select d.country_s_code,d.province_s_code,d.city_s_code,d.area_s_code,d.country,c.province_en as province,c.city_en as city,d.country_code,d.province_code,d.city_code,d.area_code,d.country_poi,c.province as province_poi,c.city as city_poi,d.area_poi,d.provincial_capital,d.country_en,d.is_hot,d.continents,d.travel_area,d.area,cast('0' as int) as city_max
from $mapping_area_new c
left join $mapping_area_code_province_new d
on c.country=d.country and c.province=d.province;

drop table $mapping_area_code_city_new;
create table $mapping_area_code_city_new stored as orc as
select b.country_s_code,b.province_s_code,b.city_s_code,b.area_s_code,b.country,b.province,b.city,b.country_code,
b.province_code,concat(b.province_code,'_',b.city_max+a.id) as city_code,b.area_code,b.country_poi,b.province_poi,
b.city_poi,b.area_poi,b.provincial_capital,b.country_en,b.is_hot,b.continents,b.travel_area,b.area
from (
    select row_number() over(partition by country,province) id,country,province,city
    from (
        select distinct country,province,city
        from $mapping_area_code_province_new_all
        where province!='0' and country!='中国' and city!='')t1
    ) a
left join $mapping_area_code_province_new_all b
on a.country=b.country and a.province=b.province and a.city=b.city;

drop table $mapping_area_par_pre;
create table $mapping_area_par_pre stored as orc as
select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city,country_code,province_code,city_code,area_code,
country_poi,province_poi,city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
from $mapping_area_code_province_new_all
where city=''
union all
select * from $mapping_area_code_city_new
union all
select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city,country_code,province_code,city_code,area_code,
country_poi,province_poi,city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
from $mapping_area_par where $flag;

drop table $mapping_area_code_more;
create table $mapping_area_code_more stored as orc as
select a.* ,b.country as bcountry, b.province as bprovince, b.city as bcity
from( select * from $mapping_area_par_pre) a
left join (
    select country,province,city from(
      select country,province,city,count(1) as cnt from(
        select country,province,city,city_code
        from $mapping_area_par_pre
        group by country,province,city,city_code)t1
      group by country,province,city)t2
    where cnt>1
) b
on a.country=b.country and a.province=b.province and a.city=b.city
;

insert overwrite table $mapping_area_par_final partition(flag=${day})
select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city,country_code,province_code,city_code,area_code,country_poi,province_poi,city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
from(
select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city,country_code,province_code,city_code,area_code,country_poi,province_poi,city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
from $mapping_area_code_more
where bcountry is null  and bcity is null
union all
select country_s_code,province_s_code,city_s_code,area_s_code,country,province,city,country_code,province_code,city_code,area_code,country_poi,province_poi,city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area
from(
  select a.country_s_code,a.province_s_code,a.city_s_code,a.area_s_code,a.country,a.province,a.city,a.country_code,a.province_code,b.city_code,a.area_code,a.country_poi,a.province_poi,a.city_poi,a.area_poi,a.provincial_capital,a.country_en,a.is_hot,a.continents,a.travel_area,a.area
  from(
    select * from $mapping_area_code_more
    where bcountry is not null and bcity is not null
    ) a
  left join(
    select * from
        ( select *, row_number() over(partition by country,province,city order by cast(split(city_code,'_')[2] as int)  asc) rn
        from  $mapping_area_code_more
        where bcountry is not null and bcity is not null
        ) t1
    where rn=1) b
  on a.country=b.country and a.province=b.province and a.city=b.city) t2
) t3
group by country_s_code,province_s_code,city_s_code,area_s_code,country,province,city,country_code,province_code,city_code,area_code,country_poi,province_poi,city_poi,area_poi,provincial_capital,country_en,is_hot,continents,travel_area,area;
"

count_new=`hive -e"desc formatted $mapping_area_par_final partition(flag=${day});"|grep 'numRows'|awk -F ' ' '{print $2}'`

# 如果 mapping_area_par_final 最新分区条数 count_new 多于 mapping_area_par 上一分区 count 则更新 mapping_ip_attribute_code

if [[ $count_new -gt $count ]];then
HADOOP_USER_NAME=dba hive -e"
insert overwrite table $mapping_ip_attribute_code partition(day=$day)
select a.id,a.minip,a.maxip,a.continent,a.areacode,a.adcode,a.country,a.province,a.city,a.district,
b.country_code,b.province_code,b.city_code,b.area_code,a.bd_lon,a.bd_lat,a.wgs_lon,a.wgs_lat,
a.radius,a.scene,a.accuracy,a.owner,a.isp,a.asnumber,a.zipcode,a.timezone
from (
    select * from $mapping_ip_attribute_code_tmp
    where country_code is null and country!='') a
left join $mapping_area_par_final b
on a.country=b.country_poi and a.province=b.province_poi and a.city=b.city_poi
union all
select id,minip,maxip,continent,areacode,adcode,country,province,city,district,'' as country_code,'' as province_code,
'' as city_code,'' as area_code,bd_lon,bd_lat,wgs_lon,wgs_lat,radius,scene,accuracy,owner,isp,asnumber,zipcode,timezone
from $mapping_ip_attribute_code_tmp
where country_code is null and country=''
union all
select id,minip,maxip,continent,areacode,adcode,country,province,city,district,country_code,province_code,
city_code,area_code,bd_lon,bd_lat,wgs_lon,wgs_lat,radius,scene,accuracy,owner,isp,asnumber,zipcode,timezone
from $mapping_ip_attribute_code_tmp
where country_code is not null
;"
else
HADOOP_USER_NAME=dba hive -e"
insert overwrite table $mapping_ip_attribute_code partition(day=$day)
select id,minip,maxip,continent,areacode,adcode,country,province,city,district,
country_code,province_code,city_code,area_code,bd_lon,bd_lat,wgs_lon,wgs_lat,radius,
scene,accuracy,owner,isp,asnumber,zipcode,timezone
from $mapping_ip_attribute_code_tmp
;"
fi
