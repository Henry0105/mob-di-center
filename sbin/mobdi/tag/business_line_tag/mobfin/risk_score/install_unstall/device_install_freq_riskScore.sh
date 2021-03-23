#!/bin/bash
: '
@owner:luost
@describe:近7、14、30天安装频率评分标签预处理
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
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
tmp_anticheat_device_install_7days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_7days_pre
tmp_anticheat_device_install_14days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_14days_pre
tmp_anticheat_device_install_30days_pre=dw_mobdi_tmp.tmp_anticheat_device_install_30days_pre

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore

#近7天评分
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '7',flag = '4')
select device,avg(riskScore) as riskScore
from 
(
    select device,ln(cnt_all/cnt_pkg)/(ln(cnt_all/cnt_pkg)+1) as riskScore
    from $tmp_anticheat_device_install_7days_pre
    where day = '$day'
)a
group by device;
"

#近14天评分
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '14',flag = '4')
select device,avg(riskScore) as riskScore
from 
(
    select device,ln(cnt_all/cnt_pkg)/(ln(cnt_all/cnt_pkg)+1) as riskScore
    from $tmp_anticheat_device_install_14days_pre
    where day = '$day'
)a
group by device;
"

#近30天评分
hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '30',flag = '4')
select device,avg(riskScore) as riskScore
from 
(
    select device,ln(cnt_all/cnt_pkg)/(ln(cnt_all/cnt_pkg)+1) as riskScore
    from $tmp_anticheat_device_install_30days_pre
    where day = '$day'
)a
group by device;
"