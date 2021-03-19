#!/bin/bash
: '
@owner:luost
@describe:反欺诈代理ip标签
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
source /home/dba/mobdi_center/conf/hive_db_tb_mobdi_mapping.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_device_ip_pre=dw_mobdi_tmp.tmp_anticheat_device_ip_pre

#mapping
#dim_ip_type_all_mf=dim_mobdi_mapping.dim_ip_type_all_mf

#输出表
#label_l1_anticheat_device_proxy_ip_label_wf=dm_mobdi_report.label_l1_anticheat_device_proxy_ip_label_wf

iptypePartition=`hive -S -e "show partitions $dim_ip_type_all_mf" | sort |tail -n 1 `
function proxy_ip(){

timewindow=$1
table=dw_mobdi_md.tmp_anticheat_device_ip_cnt_$1days
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with ip_daili as(
    select ip_risk_pre.device,ip_risk_pre.ipaddr
    from 
    (
        select clientip
        from $dim_ip_type_all_mf
        where $iptypePartition
        and type = 3
        group by clientip
    ) as ip_location 
    inner join 
    (
        select device,ipaddr
        from $tmp_anticheat_device_ip_pre
        where day = '$day'
        and connect_day <= '$day'
        and connect_day > '$pday'
        group by device,ipaddr
    ) as ip_risk_pre 
    on ip_location.clientip = ip_risk_pre.ipaddr
)

insert overwrite table $label_l1_anticheat_device_proxy_ip_label_wf partition(day = '$day',timewindow = '$timewindow',flag = 0)
select ip_daili.device,ip_daili.daili_ipcnt,ip_daili.daili_ipcnt/ip_cnt.cnt as proportion
from 
(
    select device,count(*) as daili_ipcnt
    from ip_daili
    group by device
) as ip_daili 
left join 
(
    select device,cnt
    from $table
    where day = '$day'
)ip_cnt  
on ip_daili.device = ip_cnt.device;
"
}


for i in 7 14 30
do
    proxy_ip $i
done