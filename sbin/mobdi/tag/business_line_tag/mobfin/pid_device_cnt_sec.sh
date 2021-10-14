#!/bin/bash
: '
@owner:luost
@describe:手机号关联设备数标签
@projectName:mobdi
'

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
#dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

# mapping
#dim_pid_attribute_full_par_secview=dim_mobdi_mapping.dim_pid_attribute_full_par_secview

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6g' -XX:+UseG1GC;
set mapred.job.name=pid_device_cnt_sec;

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '9')
select a.pid,count(1) as cnt
from 
(
    select device,pid
    from $dwd_log_device_info_jh_sec_di
    where day <= '$day'
    and day > '$pday'
    and pid is not null
    and pid != ''
    and device is not null
    and length(trim(device)) = 40
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
    group by pid,device
) a
left semi join    --取中国手机号
(
  select * 
  from $dim_pid_attribute_full_par_secview
  where country = '中国'
) pid_attribute 
on a.pid = pid_attribute.pid_id
group by a.pid;
"