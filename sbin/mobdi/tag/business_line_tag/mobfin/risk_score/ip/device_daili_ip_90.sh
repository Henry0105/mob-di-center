#!/bin/bash
: '
@owner:luost
@describe:近90天代理ip连接占比
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
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

#mapping
#dim_ip_type_mf=dim_mobdi_mapping.dim_ip_type_mf
#dim_ip_type_all_mf=dim_mobdi_mapping.dim_ip_type_all_mf

#tmp
tmp_anticheat_device_ip_cnt_90days=$dw_mobdi_tmp.tmp_anticheat_device_ip_cnt_90days

#输出表
#label_l1_anticheat_device_proxy_ip_label_wf=dm_mobdi_report.label_l1_anticheat_device_proxy_ip_label_wf

iptypePartition=`hive -S -e "show partitions $dim_ip_type_mf" | sort |tail -n 1 `

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.groupby.skewindata=true;
SET hive.map.aggr=true;
set hive.exec.parallel=true;
set hive.exec.reducers.bytes.per.reducer=3221225472;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

with ip_daili as(
    select device,ipaddr
    from 
    (
    	select device,ipaddr
		from $dws_device_ip_info_di
		where day > '$p90day' 
		and day <= '$day' 
		and ipaddr <> '' 
		and ipaddr is not null
		group by device,ipaddr
    )a
    left semi join
    (
        select clientip
        from $dim_ip_type_mf
        where $iptypePartition
        and type = 3
        group by clientip
    )b
    on a.ipaddr = b.clientip
)

insert overwrite table $label_l1_anticheat_device_proxy_ip_label_wf partition(day = '$day',timewindow = '90' ,flag = 0)
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
    from $tmp_anticheat_device_ip_cnt_90days
    where day = '$day'
)ip_cnt  
on ip_daili.device = ip_cnt.device;
"