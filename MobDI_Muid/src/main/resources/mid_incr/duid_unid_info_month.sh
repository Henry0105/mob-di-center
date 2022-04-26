#!/bin/bash

: '
@owner:luost
@DESCribe:mid增量合并
@projectName:MOBDI
'

set -e -x 

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p30_day=`date -d "$day -30 days" +%Y%m%d`


hive -v -e "
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

INSERT OVERWRITE TABLE dm_mid_master.duid_unid_info_month PARTITION (day = '$day')
SELECT duid
     , unid_final
     , pkg_it
FROM dm_mid_master.duid_unid_info_di
WHERE day > '$p30_day'
AND day <= '$day'
GROUP BY duid,unid_final,pkg_it;
"