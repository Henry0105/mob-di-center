#!/bin/bash
set -x -e

if [ $# -lt 1 ]; then
    echo "Please input param: day"
    exit 1
fi

insert_day=$1

# 获取当前日期所在月的第一天
start_month=$(date -d "${insert_day}  " +%Y%m01)
# 获取当前日期所在月的最后一天
end_month=$insert_day

source /home/dba/mobdi_center/conf/hive-env.sh

# input
#label_device_pkg_install_uninstall_year_info_mf=dm_mobdi_report.label_device_pkg_install_uninstall_year_info_mf

tmpdb=dm_mobdi_tmp
income_new_topic_wgt="${tmpdb}.income_new_topic_wgt"



HADOOP_USER_NAME=dba hive -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.created.files=1000000;
set mapreduce.reduce.memory.mb=5120;
set mapreduce.map.memory.mb=5120;
set mapreduce.map.java.opts=-Xmx4096m -XX:+UseConcMarkSweepGC;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.groupby.mapaggr.checkinterval=100000;
set hive.skewjoin.key=100000;
set hive.optimize.skewjoin=true;


insert overwrite table $income_new_topic_wgt partition (day='$end_month')
select device,
avg( topic_11)as topic_11,
avg( topic_16)as topic_16,
avg( topic_7)as topic_7,
avg( topic_8)as topic_8,
avg( topic_9)as topic_9,
avg( topic_0)as topic_0,
avg( topic_1)as topic_1
from(
    select pkg,topic_11,topic_16,topic_7,topic_8,topic_9,topic_0,topic_1
    from $mapping_pkg_topic_wgt
)a
inner join
(
    select device,pkg
    from $label_device_pkg_install_uninstall_year_info_mf
    where day='$end_month'
    and update_day between '$start_month' and '$end_month'
)b
on a.pkg=b.pkg
group by device;
"