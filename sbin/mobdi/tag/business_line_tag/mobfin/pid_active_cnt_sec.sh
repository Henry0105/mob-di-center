#!/bin/bash
: '
@owner:luost
@describe:90天手机号出现天数
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p90day=`date -d "$day -90 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties

#源表
#dim_id_mapping_android_sec_df=dim_mobdi_mapping.dim_id_mapping_android_sec_df
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

# mapping
#dim_pid_attribute_full_par_secview=dim_mobdi_mapping.dim_pid_attribute_full_par_secview

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec

androidPartition=`hive -S -e "show partitions $dim_id_mapping_android_sec_df" | sort | grep -v '.1000'|tail -n 1 `

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
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;
set mapred.job.name=pid_active_cnt;

with pid_device as (
    select device,mytable.pid as pid
    from
    (
        select device,pid
        from $dim_id_mapping_android_sec_df
        where $androidPartition
        and pid is not null
        and pid <> '' 
    )a
    lateral view explode(split(pid,',')) mytable as pid
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '90',flag = '7')
select pid,count(1) as cnt
from 
(
    select a.pid,b.day
    from 
    (
        select pid,device
        from pid_device b
        left semi join    
        (
            select * 
            from $dim_pid_attribute_full_par_secview
            where country = '中国'
        ) pid_attribute 
        on b.pid = pid_attribute.pid_id
        group by pid,device
    )a
    inner join
    (
        select device,day
        from $dws_device_ip_info_di
        where day <= '$day'
        and day > '$p90day'
        and device is not null 
        and length(trim(device)) = 40 
        and device = regexp_extract(device,'([a-f0-9]{40})', 0)
        group by device,day
    )b
    on a.device = b.device
    group by a.pid,b.day
)c
group by pid;
"






