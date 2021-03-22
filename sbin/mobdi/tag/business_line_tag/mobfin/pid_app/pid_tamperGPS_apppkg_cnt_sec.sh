#!/bin/bash
: '
@owner:luost
@describe:在装、安装、活跃过篡改GPS类app软件个数标签
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
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_pid_device_pre_sec=dw_mobdi_tmp.tmp_anticheat_pid_device_pre_sec
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#dws_device_active_applist_di=dm_mobdi_topic.dws_device_active_applist_di

#mapping
#dim_device_applist_new_di=dm_mobdi_mapping.dim_device_applist_new_di

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec

pidPartition=`hive -S -e "show partitions $tmp_anticheat_pid_device_pre_sec" | sort |tail -n 1 `

function tamper_GPS(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

#安装过篡改GPS类应用数
hive -v -e "
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts='-Xmx1800m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx1800m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx7400m' -XX:+UseG1GC;
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
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
set mapred.job.name=pid_tamperGPS_apppkg_cnt_sec1;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;

with install_apppkg as (
    select device,count(1) as cnt
    from
    (
        select device,pkg
        from $dwd_master_reserved_new_di
        where day <= '$day'
        and day > '$pday'
        and pkg in ('com.wx.mockgps','com.rong.xposed.fakelocation','com.wifi99.android.locationcheater','io.xudwoftencentmm','org.proxydroid','top.a1024bytes.mockloc.ca.pro','com.txy.anywheren','com.txy.anywhere694','com.deniu.multi','com.lexa.fakegps','com.qgwapp.shadowside','com.lerist.fakelocation','net.superal')
        group by device,pkg
    )a
    group by device

)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition(day = '$day',timewindow = '$timewindow',flag = '1')
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

#活跃过的篡改GPS类应用数
hive -v -e "
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts='-Xmx1800m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx1800m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx7400m' -XX:+UseG1GC;
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
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
set mapred.job.name=pid_tamperGPS_apppkg_cnt_sec2;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;

with active_apppkg as (
    select device,count(1) as cnt
    from
    (
        select device,apppkg
        from $dwd_device_active_applist_di
        where day <= '$day'
        and day > '$pday'
        and apppkg in ('com.wx.mockgps','com.rong.xposed.fakelocation','com.wifi99.android.locationcheater','io.xudwoftencentmm','org.proxydroid','top.a1024bytes.mockloc.ca.pro','com.txy.anywheren','com.txy.anywhere694','com.deniu.multi','com.lexa.fakegps','com.qgwapp.shadowside','com.lerist.fakelocation','net.superal')
        group by device,apppkg
    )a
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '11')
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
set mapreduce.map.memory.mb=2048;
set mapreduce.map.java.opts='-Xmx1800m' -XX:+UseG1GC;
set mapreduce.child.map.java.opts='-Xmx1800m';
set mapreduce.reduce.memory.mb=8192;
set mapreduce.reduce.java.opts='-Xmx7400m' -XX:+UseG1GC;
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
set mapreduce.job.reduce.slowstart.completedmaps=0.8;
set mapred.job.name=pid_tamperGPS_apppkg_cnt_sec3;
set mapreduce.reduce.shuffle.memory.limit.percent=0.15;

with installing_apppkg as (
    select device,count(1) as cnt
    from
    (
        select device,pkg
        from
        (
            select device,pkg,rank() over(partition by device order by day desc) as rk
            from $device_applist_new
            where day <= '$day'
            and day > '$pday'
        )a
        where rk = 1
        and pkg in ('com.wx.mockgps','com.rong.xposed.fakelocation','com.wifi99.android.locationcheater','io.xudwoftencentmm','org.proxydroid','top.a1024bytes.mockloc.ca.pro','com.txy.anywheren','com.txy.anywhere694','com.deniu.multi','com.lexa.fakegps','com.qgwapp.shadowside','com.lerist.fakelocation','net.superal')
        group by device,pkg
    )d
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '12')
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
}

for i in 1 7 14 30
do
    tamper_GPS $i
done
