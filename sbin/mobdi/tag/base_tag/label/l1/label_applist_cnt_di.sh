#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 计算设备的applist_cnt
@projectName:MOBDI
使用脚本: model_consume_v2.sh
'

day=$1

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"

##input
device_applist_new=${dim_device_applist_new_di}
##output
label_device_applist_cnt=${label_l1_applist_refine_cnt_di}

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set hive.optimize.index.filter=true;
set hive.exec.orc.zerocopy=true;
set hive.optimize.ppd=true;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=250000000;
set mapred.min.split.size.per.rack=250000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.exec.reducers.bytes.per.reducer=256000000;

insert overwrite table $label_device_applist_cnt partition(day=$day)
select device, count(1) as cnt, concat_ws(',',collect_set(pkg)) as applist
from $device_applist_new
where day=$day
group by device;
"
