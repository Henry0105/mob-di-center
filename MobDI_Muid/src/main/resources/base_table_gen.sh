#!/bin/bash
set -x -e
cd "`dirname $0`"
source ./hive_table_name.properties

: '
1.创建分厂商deviceid_new和各个id的mapping表
2.创建deviceid_new和deviceid_old的mapping表
3.从基准表里导数据到上面两张表
'

hive -e "
create table if not exist $deviceid_old_new_mapping (
deviceid_old string,
deviceid_new string
) comment '新旧deviceid的mapping'
partitioned by (day string)
stored as orc
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.bloom.filter.columns'='deviceid_old,deviceid_new');

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table $deviceid_old_new_mapping partitioned(day)
select deviceid_old,deviceid_new
from $base_table sort by deviceid_old;

create table if not exist $deviceid_new_ids_mapping (
deviceid_new string comment '新生成的deviceid',
ieid string,
mcid string,
snid string,
oiid string,
asid string,
token string,
sysver sring,
update_time string comment '最后活跃时间'
) comment '分厂商deviceid_new和各个id的mapping表'
partitioned by (
day string,
factory string comment '厂商,大厂各存一个分区,其他厂在other分区里')
stored as orc
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.bloom.filter.columns'='oiid,asid,ieid');

insert overwrite table $deviceid_new_ids_mapping partition(day,factory)
select deviceid_new,ieid,mcid,snid,oiid,asid,token,update_time,day,factory from (
select deviceid_new,ieid,mcid,snid,oiid,asid,token,st update_time,'20210201' day,
case when factory in ($popular_factory) then factory else 'other' end as factory
from $base_table) t sort by oiid;
"


