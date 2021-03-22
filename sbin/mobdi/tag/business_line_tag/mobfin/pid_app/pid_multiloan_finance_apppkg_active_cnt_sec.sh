#!/bin/bash
: '
@owner:luost
@describe:手机号对应多头借贷类app活跃次数标签
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
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_sdk_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_pid_device_pre_sec=dw_mobdi_tmp.tmp_anticheat_pid_device_pre_sec
#dwd_pv_sec_di=dm_mobdi_master.dwd_pv_sec_di
#dwd_log_run_new_di=dm_mobdi_master.dwd_log_run_new_di
#dwd_device_app_runtimes_sec_di=dm_mobdi_master.dwd_device_app_runtimes_sec_di
#dwd_xm_device_app_runtimes_sec_di=dm_mobdi_master.dwd_xm_device_app_runtimes_sec_di

#mapping表
#online_category_mapping_v3=dim_sdk_mapping.online_category_mapping_v3

#输出表
#label_l1_anticheat_pid_cnt_sec=dm_mobdi_report.label_l1_anticheat_pid_cnt_sec

categoryPartition=`hive -S -e "show partitions $online_category_mapping_v3" | sort |tail -n 1 `
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
set hive.heartbeat.interval=10000;
set mapred.job.name=pid_multiloan_finance_apppkg_active_cnt_sec;

with multiloan_finance_apppkg_table as (
    select relation 
    from $online_category_mapping_v3 
    where $categoryPartition
    and cate in ('P2P借贷','小额借贷','消费金融','现金贷','综合借贷')
),

dwd_pv_id_table as (
    select deviceid as device,apppkg,unix_timestamp(clienttime) as clienttime
    from
    (
        select deviceid,apppkg,clienttime
        from $dwd_pv_sec_di
        where day <= '$day'
        and day > '$pday'
        and length(trim(deviceid)) = 40
        and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
    ) a
    left semi join 
    multiloan_finance_apppkg_table b
    on a.apppkg = b.relation
),

run_new_table as (
    select deviceid as device,apppkg,substring(clienttime,1,10) as clienttime
    from
    (
        select deviceid,apppkg,clienttime
        from $dwd_log_run_new_di
        where day <= '$day'
        and day > '$pday'
        and length(trim(deviceid)) = 40
        and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
    ) a
    left semi join 
    multiloan_finance_apppkg_table b
    on a.apppkg = b.relation
),

app_runtimes_table as (
    select device,pkg as apppkg,substring(clienttime,1,10) as clienttime
    from
    (
        select device,pkg,clienttime
        from $dwd_device_app_runtimes_sec_di
        where day <= '$day'
        and day > '$pday'
        and length(trim(device)) = 40
        and device = regexp_extract(device,'([a-f0-9]{40})', 0)
    ) a
    left semi join 
    multiloan_finance_apppkg_table b
    on a.pkg = b.relation
),

dwd_xm_device_app_runtimes_di_table as (
    select deviceid as device,apppkg,substring(clienttime,1,10) as clienttime
    from
    (
        select deviceid,apppkg,clienttime
        from $dwd_xm_device_app_runtimes_sec_di
        where day <= '$day'
        and day > '$pday'
        and length(trim(deviceid)) = 40
        and deviceid = regexp_extract(deviceid,'([a-f0-9]{40})', 0)
    ) a
    left semi join 
    multiloan_finance_apppkg_table b
    on a.apppkg = b.relation
),

active_cnt_table as (
    select device,count(1) as cnt
    from
    (
        select device,apppkg
        from
        (
            select device,apppkg,clienttime
            from dwd_pv_id_table

            union all 

            select device,apppkg,clienttime
            from run_new_table

            union all

            select device,apppkg,clienttime
            from app_runtimes_table

            union all

            select device,apppkg,clienttime
            from dwd_xm_device_app_runtimes_di_table
        )a
        group by device,apppkg,clienttime
    )b
    group by device
)

insert overwrite table $label_l1_anticheat_pid_cnt_sec partition (day = '$day',timewindow = '$timewindow',flag = '5')
select a.pid,if(b.device is null,0,b.cnt) as cnt
from 
(
    select pid,device
    from $tmp_anticheat_pid_device_pre_sec
    where $pidPartition
)a
left join 
active_cnt_table b
on a.device = b.device;
"
