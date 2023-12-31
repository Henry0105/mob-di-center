#!/bin/bash
: '
@owner:luost
@describe:用户安装生活类app个数
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
tmp_anticheat_pid_device_pre_sec=$dw_mobdi_tmp.tmp_anticheat_pid_device_pre_sec
#device_profile_label_full_par=dm_mobdi_report.device_profile_label_full_par

#mapping表
#dim_app_category_mapping_par=dim_sdk_mapping.dim_app_category_mapping_par
#app_category_mapping_par=dim_sdk_mapping.app_category_mapping_par

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec

fullPartition=`hive -S -e "show partitions $device_profile_label_full_par" | sort | grep -v 'monthly_bak'|tail -n 1 `
appPartition=`hive -S -e "show partitions $dim_app_category_mapping_par" | sort |tail -n 1 `
pidPartition=`hive -S -e "show partitions $tmp_anticheat_pid_device_pre_sec" | sort |tail -n 1 `

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
set mapred.job.name=pid_install_life_apppkg_cnt_sec;

with apppkg_table as (
    select apppkg 
    from $dim_app_category_mapping_par
    where $appPartition 
    and cate_l1 = '便捷生活'
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
    select device,count(1) as cnt
    from device_pkg_full_table a
    left semi join
    apppkg_table b
    on a.pkg = b.apppkg
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '1',flag = '6')
select a.pid,if(b.device is null,0,b.cnt) as cnt
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

