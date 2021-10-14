#!/bin/bash
: '
@owner:luost
@describe:近90天连网方式评分
@projectName:mobdi
'

set -x -e

if [ $# -ne 2 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <riskOf90days>"
    exit 1
fi

day=$1
#0.8
riskOf90days=$2
p90day=`date -d "$day -90 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore


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

with netwrok_cnt_and_ifwifi as(
    select device,count(1) as network_cnt,sum(wifi_help) as ifwifi
    from 
    (
        select device,network, 
        case 
          when network = 'wifi' then 1 
        else 0 
        end as wifi_help
        from 
        (
            select device,network
            from 
            (
                select device,trim(t.network_split) as network
                from $dws_device_ip_info_di
                lateral view explode(split(network, ',')) t as network_split
                where length(trim(t.network_split)) > 0
                and day > '$p90day' 
                and day <= '$day' 
                and length(trim(network)) > 0
            ) as network_explode 
            group by device,network
        ) as b 
    ) as c 
    group by device
)

insert overwrite table $label_l1_anticheat_device_riskscore partition(day = '$day',timewindow = '90',flag = '7')
select device,$riskOf90days*ifwifi/network_cnt as riskScore
from netwrok_cnt_and_ifwifi;
"