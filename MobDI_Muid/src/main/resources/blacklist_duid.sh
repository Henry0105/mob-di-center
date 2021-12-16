#!/bin/bash
set -x -e

hive -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
drop table if exists dm_mid_master.blacklist_duid;
create table dm_mid_master.blacklist_duid stored as orc
as select duid,concat(split(pkg_it,'_')[0],'_', split(pkg_it,'_')[1]) pkg_ver,count(*) cnt
  from (
    select * from dm_mid_master.pkg_it_duid_category_tmp
    where unid is not null and pkg_it is not null
    and duid is not null and trim(duid)<>''
 ) t
  group by duid,concat(split(pkg_it,'_')[0],'_', split(pkg_it,'_')[1]);

"
