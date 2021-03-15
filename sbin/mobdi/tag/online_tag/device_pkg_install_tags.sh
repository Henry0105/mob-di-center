#!/bin/bash


: '
@owner:hugl
@describe:【新标签】在装标签支持,近一年、半年、90天、30天时间范围内安装过的app包名（清洗前）（不用考虑后续是否卸载）
@projectName:mobdi
'

set -x -e 

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#源表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#输出表
#timewindow_online_profile_install_list=dm_mobdi_report.timewindow_online_profile_install_list

year_days=`date -d "$day -365 days" +%Y%m%d`

half_year_days=`date -d "$day -180 days" +%Y%m%d`

ninety_days=`date -d "$day -90 days" +%Y%m%d`

thirty_days=`date -d "$day -30 days" +%Y%m%d`

### 一个月的日更
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $timewindow_online_profile_install_list partition (flag=23, day='$day', timewindow='30')
select device,concat_ws(',', collect_set(pkg)) as applist
from $dws_device_install_app_re_status_di
where day>'$thirty_days'
and day<='$day'
group by device
"

### 3个月的周更
sql1="
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $timewindow_online_profile_install_list partition (flag=23, day='$day', timewindow='90')
select device,concat_ws(',', collect_set(pkg)) as applist
from $dws_device_install_app_re_status_di
where day>'$ninety_days'
and day<='$day'
group by device;
"
week=`date -d ${day} +%w`
if [ ${week} -eq 1 ]; then
	hive -v -e "${sql1}"
	echo "week task finished"
fi

### 半年和一年的月更
sql2="
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $outputtable partition (flag=23, day='$day', timewindow='180')
select device,concat_ws(',', collect_set(pkg)) as applist
from $dws_device_install_app_re_status_di
where day>'$half_year_days' and day<='$day'
group by device;
"

sql3="
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $outputtable partition (flag=23, day='$day', timewindow='365')
select device,concat_ws(',', collect_set(pkg)) as applist
from $dws_device_install_app_re_status_di
where day>'$year_days'
and day<='$day'
group by device;
"

date=`date -d "${day}" +"%d"`
if [ $date -eq 1 ]; then
	hive -v -e "${sql2}"
	hive -v -e "${sql3}"
	echo "month task finished"
fi
