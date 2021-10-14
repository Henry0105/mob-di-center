#!/bin/bash
: '
@owner:luost
@describe:在装、活跃、安装过借贷类应用个数标签
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
tmp_anticheat_pid_device_pre_sec=$dw_mobdi_tmp.tmp_anticheat_pid_device_pre_sec
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di
#dim_device_applist_new_di=dim_mobdi_mapping.dim_device_applist_new_di

#mapping表
#dim_online_category_mapping_v3=dim_sdk_mapping.dim_online_category_mapping_v3
#online_category_mapping_v3=dim_sdk_mapping.online_category_mapping_v3

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec


categoryPartition=`hive -S -e "show partitions $dim_online_category_mapping_v3" | sort |tail -n 1 `
pidPartition=`hive -S -e "show partitions $tmp_anticheat_pid_device_pre_sec" | sort |tail -n 1 `

#安装过借贷类应用数
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
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;
set mapred.job.name=pid_debit_apppkg_cnt_sec;


with apppkg_table as (
    select relation
    from $dim_online_category_mapping_v3
    where $categoryPartition
    and cate in ('P2P借贷','借贷','小额借贷','优质借贷','现金贷','综合借贷','劣质借贷')
),

install_apppkg as (
    select device,count(1) as cnt
    from
    (
        select device,pkg
        from
        (
            select device,pkg
            from $dws_device_install_app_re_status_di
            where day <= '$day'
            and day > '$pday'
        )a
        left semi join
        apppkg_table b
        on a.pkg = b.relation
        group by device,pkg
    )c
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition(day = '$day',timewindow = '$timewindow',flag = '3')
select a.pid,if(b.device is null,0,b.cnt) as cnt
from
(
    select pid,device
    from $tmp_anticheat_pid_device_pre_sec
    where $pidPartition
)a
left join
install_apppkg b
on a.device = b.device;
"

#活跃过的借贷类应用数
hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3g' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx3g';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx6g' -XX:+UseG1GC;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set mapred.job.name=pid_debit_apppkg_cnt_sec;

with apppkg_table as (
    select relation
    from $dim_online_category_mapping_v3
    where $categoryPartition
    and cate in ('P2P借贷','借贷','小额借贷','优质借贷','现金贷','综合借贷','劣质借贷')
),

active_apppkg as (
    select device,count(1) as cnt
    from
    (
        select device,apppkg
        from
        (
            select device,apppkg
            from $dws_device_active_applist_di
            where day <= '$day'
            and day > '$pday'
        )a
        left semi join
        apppkg_table b
        on a.apppkg = b.relation
        group by device,apppkg
    )c
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '15')
select a.pid,if(b.device is null,0,b.cnt) as cnt
from
(
    select pid,device
    from $tmp_anticheat_pid_device_pre_sec
    where $pidPartition
)a
left join
active_apppkg b
on a.device = b.device;
"

#在装的借贷类应用数
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
set mapreduce.reduce.memory.mb=10240;
set mapreduce.reduce.java.opts='-Xmx8g' -XX:+UseG1GC;
set mapreduce.reduce.shuffle.parallelcopies=5;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;
set mapred.job.name=pid_debit_apppkg_cnt_sec;

with apppkg_table as (
    select relation
    from $dim_online_category_mapping_v3
    where $categoryPartition
    and cate in ('P2P借贷','借贷','小额借贷','优质借贷','现金贷','综合借贷','劣质借贷')
),

installing_apppkg as (
    select device,count(1) as cnt
    from
    (
        select device,pkg
        from
        (
            select device,pkg
            from
            (
                select device,pkg,rank() over(partition by device order by day desc) as rk
                from $dim_device_applist_new_di
                where day <= '$day'
                and day > '$pday'
            )a
            where rk = 1
        )b
        left semi join
        apppkg_table c
        on b.pkg = c.relation
        group by device,pkg
    )d
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '16')
select a.pid,if(b.device is null,0,b.cnt) as cnt
from
(
    select pid,device
    from $tmp_anticheat_pid_device_pre_sec
    where $pidPartition
)a
left join
installing_apppkg b
on a.device = b.device;
"
