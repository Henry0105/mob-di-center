#!/bin/bash
: '
@owner:luost
@describe:用户是否安装微信或QQ
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
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_pid_device_pre_sec=dw_mobdi_tmp.tmp_anticheat_pid_device_pre_sec
#device_profile_label_full_par=dm_mobdi_report.device_profile_label_full_par

#mapping表
#app_category_mapping_par=dim_sdk_mapping.app_category_mapping_par

#输出表
#label_l1_anticheat_pid_install_wechat_qq_sec_df=dm_mobdi_report.label_l1_anticheat_pid_install_wechat_qq_sec_df

fullPartition=`hive -S -e "show partitions $device_profile_label_full_par" | sort | grep -v 'monthly_bak'|tail -n 1 `
appPartition=`hive -S -e "show partitions $app_category_mapping_par" | sort |tail -n 1 `
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
set hive.exec.reducers.bytes.per.reducer=2147483648;
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
set mapreduce.job.reduce.slowstart.completedmaps=0.8;

SET hive.exec.max.created.files=300000;
set mapred.job.name=pid_install_wechat_qq_sec;

with apppkg_table as (
    select apppkg 
    from $app_category_mapping_par 
    where $appPartition 
    and appname in ('QQ','微信')
),

device_pkg_full_table as (
    select device,pkg
    from $device_profile_label_full_par
    lateral view explode(split(applist,',')) t as pkg
    where $fullPartition
    and applist <> 'unknown'
    and applist <> ''
),

install_table as (
    select device
    from device_pkg_full_table a
    left semi join
    apppkg_table b
    on a.pkg = b.apppkg
    group by device
)

insert overwrite table $label_l1_anticheat_pid_install_wechat_qq_sec_df partition (day = '$day')
select a.pid,if(b.device is null,'0','1') as flag
from 
(
    select pid,device
    from $tmp_anticheat_pid_device_pre_sec
    where $pidPartition
)a
left join 
install_table b
on a.device = b.device;
"
