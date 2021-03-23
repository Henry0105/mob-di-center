#!/bin/bash
: '
@owner:luost
@describe:设备关联手机号数标签
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
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#源表
#dwd_log_device_info_jh_sec_di=dm_mobdi_master.dwd_log_device_info_jh_sec_di

#属性表
#dim_pid_attribute_full_par_secview=dim_mobdi_mapping.dim_pid_attribute_full_par_secview

#输出表
#label_l1_anticheat_device_cnt=dm_mobdi_report.label_l1_anticheat_device_cnt

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with device_pid_table as
(
    select device,pid
    from $dwd_log_device_info_jh_sec_di
    where day <= '$day'
    and day > '$pday'
    and pid is not null
    and length(trim(pid)) > 0
    and device is not null 
    and length(trim(device)) = 40 
    and device = regexp_extract(device,'([a-f0-9]{40})', 0)
)

insert overwrite table $label_l1_anticheat_device_cnt partition (day = '$day',timewindow = '$timewindow',flag = '4')
select device,count(1) as cnt
from 
(
    select a.device
    from device_pid_table a
    left semi join
    (
        select pid_id
        from $dim_pid_attribute_full_par_secview
        where country = '中国'
    ) b
    on a.pid = b.pid_id
    group by a.device,a.pid
)a
group by device;
"