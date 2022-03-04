#!/bin/bash

set -e -x

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

day=$1

sql="
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;

set mapreduce.job.queuename=root.yarn_data_compliance;

insert overwrite table dim_mobdi_mapping.dim_device_applist_new_di partition(day=$day)
select device,COALESCE(b.apppkg,a.pkg) as pkg,$day as processtime
from
(
  select device,pkg
  from dm_mobdi_topic.dws_device_install_app_re_status_di
  where day = $day
  and refine_final_flag <> -1
) a
left join
(
  select *
  from dm_sdk_mapping.app_pkg_mapping_par
  where version='1000'
  and apppkg <> ''
  and apppkg is not null
) b on (a.pkg = b.pkg)
group by device,COALESCE(b.apppkg,a.pkg);
"

echo "$sql"

HADOOP_USER_NAME=dba hive -v -e "$sql"
