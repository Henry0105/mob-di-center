#!/bin/bash
: '
@owner:luost
@describe:营销线设备安装各类app平均天数
@projectName:mobdi
'

set -x -e

if [ $# -ne 5 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <timewindow> <pkg_cate_table> <device_table> <flag>"
    exit 1
fi

#入参
day=$1
#时间窗
timewindow=$2
#需要计算的pkg表
pkg_cate_table=$3
#需要计算特征的设备表
device_table=$4
#在timewindow_online_profile_v2表中的对应的flag
flag=$5

pday=`date -d "$day -$timewindow days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#输入表
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di

#输出表
#timewindow_online_profile_v2=dm_mobdi_report.timewindow_online_profile_v2

#取需要计算的pkg表的最新分区
pkgdb=${pkg_cate_table%.*}
pkgtable=${pkg_cate_table#*.}
pkg_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('$pkgdb', '$pkgtable', 'day');
"
pkg_mapping_partition=(`hive -e "$pkg_mapping_sql"`)

#取需要计算的设备表最新分区
devicedb=${device_table%.*}
devicetable=${device_table#*.}
device_mapping_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('$devicedb', '$devicetable', 'day');
"
device_mapping_partition=(`hive -e "$device_mapping_sql"`)

hive -v -e "
SET hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;

insert overwrite table $timewindow_online_profile_v2 partition(day = '$day',timewindow = '$timewindow',flag = '$flag')
select device,concat_ws('_',cate_id,'$flag','$timewindow'),avg(install_days) as cnt
from 
(
    select a.device,b.cate_id,datediff(a.max_day,a.min_day) as install_days
    from 
    (
        select device,pkg,
               to_date(from_unixtime(unix_timestamp(max(day), 'yyyyMMdd'))) as max_day, 
               to_date(from_unixtime(unix_timestamp(min(day), 'yyyyMMdd'))) as min_day 
        from $dws_device_install_app_re_status_di
        where day <= '$day'
        and day > '$pday'
        group by device,pkg
    )a
    inner join
    (
        select pkg,cate_id
        from $pkg_cate_table
        where day = $pkg_mapping_partition
    )b
    on a.pkg = b.pkg
    inner join
    (
        select *
        from $device_table
        where day = $device_mapping_partition
    )c
    on a.device = c.device
)d
group by device,cate_id;
"



