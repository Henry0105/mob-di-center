#!/bin/bash

set -e -x
if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<day>'"
     exit 1
fi
day=$1
HADOOP_USER_NAME=dba hive -e"
set hive.exec.parallel=true;
SET mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx6144M' -XX:+UseG1GC;;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6144M';
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.support.quoted.identifiers=None;
insert overwrite table mobdi_muid_dashboard.muid_dau_report_by_dws_device_sdk_run_master_di partition(day=$day)
select
sum(if(muid_type='muid_dau' and plat = 1,1,0)) as muid_dau_android,
sum(if(muid_type='muid_dau' and plat = 2,1,0)) as muid_dau_ios,
sum(if(muid_type='ieid_in_muid' and plat = 1,1,0)) as ieid_in_muid,
sum(if(muid_type='oiid_in_muid' and plat = 1,1,0)) as oiid_in_muid,
sum(if(muid_type='pid_in_muid_android' and plat = 1,1,0)) as pid_in_muid_android,
sum(if(muid_type='applist_in_muid' and plat = 1,1,0)) as applist_in_muid,
sum(if(muid_type='base_in_muid' and plat = 1,1,0)) as base_in_muid_andriod,
sum(if(muid_type='gps_in_muid_android' and plat = 1,1,0)) as gps_in_muid_android,
sum(if(muid_type='gps_in_muid_ios' and plat = 2,1,0)) as gps_in_muid_ios,
sum(if(muid_type='wifi_in_muid_android' and plat = 1,1,0)) as wifi_in_muid_android,
sum(if(muid_type='wifi_in_muid_ios' and plat = 2,1,0)) as wifi_in_muid_ios,
sum(if(muid_type='gps_base_wifi_in_muid_android' and plat = 1,1,0)) as gps_base_wifi_in_muid_android,
sum(if(muid_type='gps_base_wifi_in_muid_ios' and plat = 2,1,0)) as gps_base_wifi_in_muid_ios,
sum(if(muid_type='pid_in_muid_ios' and plat = 2,1,0)) as pid_in_muid_ios,
sum(if(muid_type='isid_in_muid' and plat = 1,1,0)) as isid_in_muid,
sum(if(muid_type='ifid_in_muid' and plat = 2,1,0)) as ifid_in_muid
from mobdi_muid_dashboard.muid_dau_other_id_percent_statement_by_dws_device_sdk_run_master_di 
where day=$day
"

