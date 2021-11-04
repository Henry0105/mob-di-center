#!/bin/bash
: '
@owner:luost
@describe:手机号关联ip数量标签
@projectName:mobdi
'

:<<!
create table if not exists rp_mobdi_app.label_l1_anticheat_pid_cnt_sec(
pid                          string comment '手机号',
cnt                          string comment '个数/次数'
)comment '反欺诈标签中pid为主键各类标签数据表'
partitioned by (day string comment'日期',timewindow string comment'时间窗',flag string comment'标志')
stored as orc;
!


set -x -e

if [ $# -ne 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <timewindow>"
    exit 1
fi

day=$1
timewindow=$2
pday=`date -d "$day -$timewindow days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
tmp_anticheat_pid_device_pre_sec=${dm_mobdi_tmp}.tmp_anticheat_pid_device_pre_sec
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec

pidPartition=`hive -S -e "show partitions $tmp_anticheat_pid_device_pre_sec" | sort |tail -n 1 `

hive -v -e "
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts='-Xmx1800m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx1800m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3700m' -XX:+UseG1GC;
set hive.optimize.skewjoin=true;
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
SET hive.auto.convert.join=true;
set hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.reuse.jvm.num.tasks=10;
set mapred.tasktracker.map.tasks.maximum=24;
set mapred.tasktracker.reduce.tasks.maximum=24;
set mapred.job.name=pid_ip_cnt_sec;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '10')
select pid,if(b.device is null,0,b.cnt) as cnt
from
(
    select pid,device
    from $tmp_anticheat_pid_device_pre_sec
    where $pidPartition
)a
left join
(
    select device,count(1) as cnt
    from
    (
        select device,ipaddr
        from $dws_device_ip_info_di
        where day <= '$day'
        and day > '$pday'
        and ipaddr is not null
        and ipaddr <> ''
        and device is not null
        and length(trim(device)) = 40
        and device = regexp_extract(device,'([a-f0-9]{40})', 0)
        group by device,ipaddr
    )t1
    group by device
)b
on a.device = b.device;
"
