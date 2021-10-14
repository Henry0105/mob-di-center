#!/bin/bash
: '
@owner:luost
@describe:连网方式评分标签
@projectName:mobdi
'

set -x -e

if [ $# -ne 4 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day> <riskOf7days> <riskOf14days> <riskOf30days>"
    exit 1
fi

day=$1
riskOf7days=$2
riskOf14days=$3
riskOf30days=$4

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh

#源表
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore

function network_riskScore(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`
input=$2

hive -v -e "
set hive.exec.parallel=true;
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
                and day > '$pday' 
                and day <= '$day' 
                and length(trim(network)) > 0
            ) as network_explode 
            group by device,network
        ) as b 
    ) as c 
    group by device
)

insert overwrite table $label_l1_anticheat_device_riskScore partition(day = '$day',timewindow = '$timewindow',flag = '7')
select device,$input*ifwifi/network_cnt as riskScore
from netwrok_cnt_and_ifwifi;
"
}

for i in 7 14 30
do

    if [ $i -eq 7 ]
    then
        risk=$riskOf7days
    elif [ $i -eq 14 ]
    then
        risk=$riskOf14days
    else
        risk=$riskOf30days
    fi

    network_riskScore $i $risk

done


