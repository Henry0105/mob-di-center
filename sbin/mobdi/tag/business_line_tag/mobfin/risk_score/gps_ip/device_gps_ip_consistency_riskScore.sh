#!/bin/bash
: '
@owner:luost
@describe:gps,ip一致性评分标签
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

#入参
day=$1
p30day=`date -d "$day -30 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_device_gps_ip_location=dw_mobdi_tmp.tmp_anticheat_device_gps_ip_location

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore

function device_gps_ip(){

timewindow=$1
pday=`date -d "$day -$1 days" +%Y%m%d`

hive -v -e "
SET hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=200000000;

with device_gps_ip_pre as (
    select device, sum(cnt) as cnt_all, sum(cnt_effective) as cnt_effective_all
    from 
    (
        select device,cnt,help,weigth,cnt*weigth as cnt_effective
        from 
        (
            select device,cnt,help, 
            case 
              when help = 3 then 1
              when help = 2 then 0.5
              when help = 1 then 0.2
            else 0
            end as weigth
            from 
            (
                select device,cnt,(country_help + province_help + city_help) as help
                from 
                (
                    select device,cnt, 
                    case 
                      when country_code_ip = country_code_gps then 1
                    else 0
                    end as country_help,
                    case 
                      when province_code_ip = province_code_gps then 1
                    else 0
                    end as province_help,
                    case 
                      when city_code_ip = city_code_gps then 1
                    else 0
                    end as city_help
                    from $tmp_anticheat_device_gps_ip_location
                    where day = '$day'
                    and timewindow = '$timewindow'
                ) a 
            ) b 
        ) c 
    ) d 
    group by device
)

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '$timewindow',flag = '9')
select device,1 - (cnt_effective_all/cnt_all) as riskScore
from device_gps_ip_pre;
" 
}

for i in 7 14 30
do
    device_gps_ip $i
done