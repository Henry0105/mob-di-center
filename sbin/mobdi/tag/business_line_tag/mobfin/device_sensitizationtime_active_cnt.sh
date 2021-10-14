#!/bin/bash
: '
@owner:luost
@describe:敏感时间段（2-5点）设备活跃次数
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
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di

#mapping表
#vacation_flag_par=dim_sdk_mapping.vacation_flag_par
#dim_vacation_flag_par=dm_sdk_mapping.dim_vacation_flag_par
vacation_flag_db=${dim_vacation_flag_par%.*}
vacation_flag_tb=${dim_vacation_flag_par#*.}
sql="
add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
SELECT GET_LAST_PARTITION('$vacation_flag_db', '$vacation_flag_tb', 'version');
drop temporary function GET_LAST_PARTITION;
"
vacation_flag_lastday=(`hive  -e "$sql"`)

#输出表
#label_l1_anticheat_device_cnt=dm_mobdi_report.label_l1_anticheat_device_cnt

function workday(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with device_time_table as 
(
    select deviceid,a.day as day,hours,b.day as vacation_day,b.flag
    from 
    (
        select deviceid,
               from_unixtime(cast(clienttime as bigint), 'yyyyMMdd') as day,
               from_unixtime(cast(clienttime as bigint), 'HH') as hours
        from
        (
            select deviceid,unix_timestamp(clienttime) as clienttime
            from $dwd_pv_sec_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        
            union all
        
            select deviceid,substring(clienttime, 1, 10) as clienttime
            from $dwd_log_run_new_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        )t1
        group by deviceid,clienttime
    )a
    left join 
    (
        select day,flag
        from $dim_vacation_flag_par
        where version = '1000'
    ) b
    on a.day = b.day
)

insert overwrite table $label_l1_anticheat_device_cnt partition (day = '$day',timewindow = '$timewindow',flag = '5')
select deviceid as device,sum(active) as cnt
from 
(
    select deviceid,
           if(
             pmod(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(day,'yyyyMMdd'))),'2018-01-01'),7) +1 not in (6,7)
             and hours >= 2 
             and hours < 5 
             and (vacation_day is null or flag = 3),
             1,
             0) as active
    from device_time_table
)a
group by deviceid;
"
}


function vacation(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with device_time_table as 
(
    select deviceid,a.day as day,hours,b.day as vacation_day,b.flag
    from 
    (
        select deviceid,
               from_unixtime(cast(clienttime as bigint), 'yyyyMMdd') as day,
               from_unixtime(cast(clienttime as bigint), 'HH') as hours
        from
        (
            select deviceid,unix_timestamp(clienttime) as clienttime
            from $dwd_pv_sec_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        
            union all
        
            select deviceid,substring(clienttime, 1, 10) as clienttime
            from $dwd_log_run_new_di
            where day <= '$day'
            and day > '$pday'
            and clienttime is not null
            and clienttime <> ''
            and length(trim(deviceid)) = 40
            and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
        )t1
        group by deviceid,clienttime
    )a
    left join 
    (
        select day,flag
        from $dim_vacation_flag_par
        where version = '1000'
    ) b
    on a.day = b.day
)

insert overwrite table $label_l1_anticheat_device_cnt partition (day = '$day',timewindow = '$timewindow',flag = '6')
select deviceid as device,sum(active) as cnt
from 
(
    select deviceid,
           if(
             (pmod(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(day,'yyyyMMdd'))),'2018-01-01'),7) +1 in (6,7) or flag in (1,2))
             and hours >= 2 
             and hours < 5,
             1,
             0) as active
    from device_time_table
)a
group by deviceid;
"
}


for i in 7 14 30
do
    workday $i

    vacation $i
done


