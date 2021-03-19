#!/bin/bash
: '
@owner:luost
@describe:亲友/工作共网关系准备
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties

#源表
#poi_config_mapping_par=dm_sdk_mapping.poi_config_mapping_par

#输出表
tmp_home_geohash_info=dm_mobdi_tmp.tmp_home_geohash_info
tmp_work_geohash_info=dm_mobdi_tmp.tmp_work_geohash_info

#亲友/工作地公用中间表建立
hive -v -e "
create table if not exists $tmp_home_geohash_info (
    lat string comment '纬度',
    lon string comment '经度',
    geohash6 string comment 'geohash6编码'
)
comment '亲友一度关系，小区poi信息'
partitioned by (day string comment '日期')
stored as orc;

create table if not exists $tmp_work_geohash_info (
    lat string comment '纬度',
    lon string comment '经度',
    geohash6 string comment 'geohash6编码'
)
comment '亲友一度关系，工作地poi信息' 
partitioned by (day string comment '日期')
stored as orc;
"

#写小区poi表数据
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash_adjacent as 'com.youzu.mob.java.udf.GeohashAdjacent';

--小区poi表
insert overwrite table $tmp_home_geohash_info partition (day = '$day')
select lat,lon,n.geohash6_split as geohash6
from
(
    select lat,lon,get_geohash_adjacent(geohash6) as geohash6_list
    from $poi_config_mapping_par
    where version='1001' 
    and type = 5 
    and attribute not rlike '.*写字楼|商铺.*'
)a lateral view explode(split(geohash6_list, ',')) n as geohash6_split
group by lat,lon,geohash6_split;
"

#写工作地poi表数据
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function get_geohash_adjacent as 'com.youzu.mob.java.udf.GeohashAdjacent';

--工作地poi表:周围一圈geohash6(工作地+小区的写字楼)
insert overwrite table $tmp_work_geohash_info partition (day = '$day')
select lat,lon,geohash6_split as geohash6
from
(
    select lat,lon,n.geohash6_split
    from
    (
        select lat,lon,get_geohash_adjacent(geohash6) as geohash6_list
        from $poi_config_mapping_par
        where version = '1001' 
        and type = 5 
        and attribute rlike '.*写字楼|商铺.*'
    )a lateral view explode(split(geohash6_list, ',')) n as geohash6_split

    union all

    select lat,lon,n.geohash6_split
    from
    (
        select lat,lon,get_geohash_adjacent(geohash6) as geohash6_list
        from $poi_config_mapping_par
        where version = '1001' 
        and type = 7 
    )b lateral view explode(split(geohash6_list, ',')) n as geohash6_split
)x 
group by lat,lon,geohash6_split;
"