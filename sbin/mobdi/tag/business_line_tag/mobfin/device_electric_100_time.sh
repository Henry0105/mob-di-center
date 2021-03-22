#!/bin/bash
: '
@owner:luost
@describe:设备电量百分百时间
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#dwd_light_electric_info_sec_di=dm_mobdi_master.dwd_light_electric_info_sec_di

#输出表
#label_l1_anticheat_device_electric_100_time_wi=dm_mobdi_report.label_l1_anticheat_device_electric_100_time_wi

function electric_100_time(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags';
create temporary function listtostring as 'com.youzu.mob.java.udf.ListsToString';

with batterystate_level_time as 
(
    select deviceid,mytable.col1 as batterystate_level,cast(substring(mytable.col2, 1, 10) as bigint) as time
    from 
    (
        select deviceid,concat_ws('=',listtostring(batterystate_level,time,1),listtostring(batterystate_level,time,0)) as list
        from $dwd_light_electric_info_sec_di
        where day <= '$day'
        and day > '$pday'
        and length(trim(deviceid)) = 40
        and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        and batterystate_level is not null
        and time is not null
    )a 
    lateral view explode_tags(list) mytable as col1, col2
)

insert overwrite table $label_l1_anticheat_device_electric_100_time_wi partition (day = '$day',timewindow = '$timewindow',flag = 0)
select deviceid as device,sum(timediff) as duration
from 
(
    select deviceid,
           if(batterystate_level = '100' and split(list,',')[0] = '100',split(list,',')[1] - time,0) as timediff
    from 
    (
        select deviceid,batterystate_level,time,
               lead(concat(batterystate_level,',',time),1,-1) over(partition by deviceid order by time) as list
        from batterystate_level_time
    )a
)b
group by deviceid;
"
}

for i in 7 14 30
do
    electric_100_time $i
done
