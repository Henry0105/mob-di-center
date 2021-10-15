#!/bin/bash

set -e -x

: '
@owner:luost
@describe:学历标签part2
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
tmpdb=${dw_mobdi_md}

#input
device_applist_new=${dim_device_applist_new_di}

#mapping
#mapping_edu_app_index0=dim_sdk_mapping.mapping_edu_app_index0

#ouput
tmp_edu_score_part2=${tmpdb}.tmp_edu_score_part2

:<<!
建表语句
CREATE TABLE dw_mobdi_md.tmp_edu_score_part2(
  device string, 
  index array<int>, 
  cnt array<double>)
stored as orc;

需求jira:http://j.mob.com/browse/MOBDI-2445

20210303修改表结构，为了可并行执行
CREATE TABLE dw_mobdi_md.tmp_edu_score_part2(
  device string,
  index array<int>,
  cnt array<double>)
partitioned by (day string)
stored as orc;
!

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with seed as (
  select device,
         pkg 
  from $device_applist_new 
  where day='$day'
)

insert overwrite table $tmp_edu_score_part2 partition (day = '$day')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from 
(
    select a.device, 
           b.index,
           1.0 cnt
    from seed a 
    inner join 
    (
        select apppkg, 
               index 
        from $mapping_edu_app_index0
        where version = '1000'
    )b 
    on a.pkg = b.apppkg 
)c
group by device;
"

for old_version in `hive -e "show partitions ${tmp_edu_score_part2} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${tmp_edu_score_part2} drop if exists partition($old_version)"
done
