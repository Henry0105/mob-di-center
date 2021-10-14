#!/bin/bash
set -e -x
: '
@owner:liuyanqiang
@describe:city_level
@projectName:mobdi
@BusinessName:model_age
'

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
source /home/dba/mobdi_center/conf/hive-env.sh

## input
label_merge_all=$dw_mobdi_md.model_merge_all_features

## output
outputTable="${label_l2_result_scoring_di}"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
insert overwrite table $outputTable partition(day=${day}, kind='city_level')
select device, city_level_1001 as city_level, 1.0 as probability
from $label_merge_all
where day = ${day}
and city_level_1001 is not null;
"
