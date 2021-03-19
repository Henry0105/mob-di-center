#!/bin/bash
set -x -e
: '
@owner:luost
@describe:近N天活跃天数标签
@projectName:MOBDI
'

if [ $# -ne 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <timewindow>"
    exit 1
fi

#入参
day=$1
timewindow=$2
pday=$(date -d "$day -$timewindow day" +%Y%m%d)

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#input
#device_applist_new=dm_mobdi_mapping.device_applist_new
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di
#dws_device_app_runtimes_di=dm_mobdi_topic.dws_device_app_runtimes_di
#dwd_device_info_di=dm_mobdi_master.dwd_device_info_di

#output
#timewindow_online_profile_v2=dm_mobdi_report.timewindow_online_profile_v2

feature="active_21_"$timewindow
#统计时间窗为7天的device实际活跃天数
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $timewindow_online_profile_v2 partition (flag = 21,day = '$day',timewindow='$timewindow')
select device,'$feature' as feature,count(1) as cnt
from 
(   
    select device
    from
    (
        select device,day
        from $device_applist_new
        where day > '$pday' and day <= '$day'
        group by device,day
    
        union all
    
        select device,day
        from $dws_device_ip_info_di
        where day > '$pday' and day <= '$day'
        group by device,day

        union all

        select device,day
        from $dws_device_app_runtimes_di
        where day > '$pday' and day <= '$day'
        group by device,day

        union all

        select device,day
        from $dwd_device_info_di
        where day > '$pday' and day <= '$day'
        group by device,day
    )t1
    group by device,day
)t2
group by device;
"