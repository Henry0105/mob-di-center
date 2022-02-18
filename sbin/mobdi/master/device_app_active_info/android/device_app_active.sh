#!/bin/sh
set -x -e

if [ $# -lt 1 ]; then
  echo "ERROR: wrong number of parameters"
  echo "USAGE: <date>"
  exit 1
fi

: '
@owner:xdzhang
@describe:生成设备app的日、周、月活跃报表
@projectName:MOBDI
@BusinessName:stg_base
'

#入参
day=$1

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#input
#dws_device_active_di=dm_mobdi_topic.dws_device_active_di

#mapping
app_pkg_mapping_par=dm_sdk_mapping.app_pkg_mapping_par

#output
dws_device_app_runtimes_di=dm_mobdi_topic.dws_device_app_runtimes_di
app_active_daily=dm_mobdi_report.app_active_daily
app_active_weekly=dm_mobdi_report.app_active_weekly
app_active_monthly=dm_mobdi_report.app_active_monthly


HADOOP_USER_NAME=dba hive -v -e"
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $dws_device_app_runtimes_di partition(day='$day')
select device,
       pkg,
       sum(tot_times) as tot_times,
       sum(active_cnt) as active_cnt,
       sum(front_active_cnt) as front_active_cnt,
       sum(back_active_cnt) as back_active_cnt,
       '$day' as processtime
from $dws_device_active_di
where day='${day}'
and source='runtimes'
group by device,pkg
cluster by device;
"

: '
@part_2
功能描述：1.dm_mobdi_master.app_run_times_master与dm_sdk_mapping.app_pkg_mapping_par的最新分区关联, 得到每天的设备app活跃数据,
            结果存入rp_mobdi_app.app_active_daily表
        2.rp_mobdi_app.app_active_daily取前七天数据, 存入rp_mobdi_app.app_active_weekly表, 生成周活跃报表
        3.每月的三号把rp_mobdi_app.app_active_daily表上一个整月的数据, 存入rp_mobdi_app.app_active_monthly, 生成月活跃报表
修改：202103波纹项目将 rp_mobdi_app库换为dm_mobdi_report库
'

HADOOP_USER_NAME=dba hive -e"
set mapred.max.split.size=125000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.map.aggr=true;
set hive.auto.convert.join=true;

insert overwrite table $app_active_daily PARTITION(day=${day})
select device, apppkg
from
(
  select device as device, COALESCE(b.apppkg,a.pkg) as apppkg,a.day as day
  from
  (
    select c.device,c.pkg, c.day
    from $dws_device_app_runtimes_di c
    where c.day = ${day}
  ) a
  left outer join
  (
    select *
    from $app_pkg_mapping_par
    where version='1000'
  ) b on (a.pkg=b.pkg)
  where COALESCE(b.apppkg,a.pkg) is not null
  and length(trim(COALESCE(b.apppkg,a.pkg)))>1
)dd
group by device, apppkg;
"

b7day=`date -d "$day -7 days" "+%Y%m%d"`
HADOOP_USER_NAME=dba hive -e"
set mapred.max.split.size=125000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.map.aggr=true;
set hive.auto.convert.join=true;

insert overwrite table $app_active_weekly partition(par_time=${day})
select device,
       apppkg,
       count(1) as days,
       concat_ws(',',collect_list(d.day)) as day_list
from
(
  select device,apppkg,day
  from $app_active_daily
  where day>$b7day
  and day<=$day
)d
group by device,apppkg;
"

DAY=`date -d $day  +%d`
if [ $DAY -eq 3 ];then
 nowdate=`date -d $day +%Y%m01`
startdate=`date -d"$nowdate last month" +%Y%m%d`
enddate=`date -d"$nowdate last day" +%Y%m%d`
month=`date -d"$nowdate last month" +%Y%m`
HADOOP_USER_NAME=dba hive -e"
set mapred.max.split.size=125000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 125000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.map.aggr=true;
set hive.auto.convert.join=true;

insert overwrite table $app_active_monthly PARTITION(month=$month)
select device,
       apppkg,count(1) as days,
       concat_ws(',',collect_list(d.day)) as day_list
from
(
  select device,apppkg,day
  from $app_active_daily
  where day>=$startdate
  and day<=$enddate
)d
group by device,apppkg;
"
fi


#: '
#@part_3 清除历史过期的数据
#功能描述：1.清除历史过期的数据, 保留周活近15天或者是周日的分区、保留日活近100天的分区、保留所有自然月活跃信息
#'

#b40day=`date -d "$day -100 days" +%Y%m%d`
#dpart=` hive -e "set hive.cli.print.header=flase; show partitions $app_active_daily;"`
#for var1 in $dpart;
# do
#	 dayarr=(${var1//=/ })
#	 if [ $b40day -eq ${dayarr[1]} ]; then
#		  HADOOP_USER_NAME=dba hive -e "alter table $app_active_daily drop IF EXISTS partition($var1);"
#	 fi
# done

#wait

#b15day=`date -d "$day -15 days" +%Y%m%d`
#part=` hive -e "set hive.cli.print.header=flase; show partitions $app_active_weekly;"`
#for var in $part;
#do
# weekarr=(${var//=/ });
# weekDay=`date -d "${weekarr[1]}" +%w`
# if [ $weekDay -ne 0 ] && [ $b15day -gt ${weekarr[1]} ]; then
#   HADOOP_USER_NAME=dba hive -e "alter table $app_active_weekly drop IF EXISTS partition($var);"
# fi
#done

#wait

#echo "device_app_active handle over"
