#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的bssid_cnt
@projectName:MOBDI
@update:20210323,将脚本拆分
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
b7day=`date -d "$day -7 days" "+%Y%m%d"`

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
appdb=$dm_mobdi_report

#input
device_applist_new=${dim_device_applist_new_di}

#output
output_table=${tmpdb}.tmp_score_part3

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table ${output_table} partition(day='${day}')
select device,
       collect_set(0) as index,
       collect_set(0.0) as cnt
from
(
  select device
  from $device_applist_new
  where day = '$day'
)a
group by device;
"