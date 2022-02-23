#!/bin/bash

set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <insert_day>"
  exit 1
fi

day=$1
p1day=`date -d "$day -1 days" +%Y%m%d`

# input
android_id_mapping_1to1_sec_di=dm_mobdi_tmp.android_id_mapping_1to1_sec_di
log_device_info_pre_sec_di=dm_mobdi_tmp.log_device_info_pre_sec_di

# output
android_id_mapping_1to2_sec_di=dm_mobdi_tmp.android_id_mapping_1to2_sec_di


HADOOP_USER_NAME=dba hive -v -e"
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts='-Xmx5200m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx5200m';
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts='-Xmx5200m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $android_id_mapping_1to2_sec_di partition(day='$day')
select device,ieid
from(
     select device,ieid,count(1) over (partition by ieid) as d_cnt
        from(
        select device,ieid
        from (
            select device, ieid from  $android_id_mapping_1to1_sec_di where day='$p1day'
            union all
            select device,ieid_split as ieid from $log_device_info_pre_sec_di lateral view explode(split(ieid,',')) t as ieid_split
             where day = '$day' and ieid is not null and length(trim(ieid))>0
        )t1
        group by device,ieid
     ) t2
 ) t3
where d_cnt=2;
"