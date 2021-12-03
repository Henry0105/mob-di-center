#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi
source /home/dba/mobdi_center/conf/hive-env.sh

day=$1
p7=$(date -d "$day -7 days" "+%Y%m%d")
insertday=${day}_muid

tmpdb=$dm_mobdi_tmp
gender_feature_v2_part2="${tmpdb}.gender_feature_v2_part2"

gender_feature_v2_part14="${tmpdb}.gender_feature_v2_part14"

hive -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=10000;
insert overwrite table $gender_feature_v2_part14 partition(day='$insertday')
select device,
car, game, male_other, female_high, edu, female_other, 
(car + game + male_other) male_cnt, (female_high + edu + female_other) female_cnt,
case when (car + game + male_other)=0 and (female_high + edu + female_other)>0 then 0 
when (female_high + edu + female_other)=0 and (car + game + male_other)>0 then 2 
when (female_high + edu + female_other)=0 and (car + game + male_other)>0 then -1 
else (car + game + male_other)/(cast ((female_high + edu + female_other) as float)) end tgi_male_female 
from (
select t1.device, 
(index158 + index159 + index165 + index162 + index163 + index96 + index166) car,
(index20 + index152 + index180 + index178 + index209 + index207 + index176 + index177 + index201) game,
(index102+index124+index147+index170+index24+index100+index2+index167+index10+index121+index197) male_other,
(index115 + index57 + index120 + index59 + index130 + index117) female_high,
(index87+index93+index91+index86+index88+index94+index92) edu,
(index42+index16+index188+index64+index52+index112+index128+index14+index47+index43) female_other
from $gender_feature_v2_part2 t1 where day='$insertday'
) t
"