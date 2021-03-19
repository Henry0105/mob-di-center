#!/bin/bash
: '
@owner:luost
@describe:近7、14、30天安装评分标签
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
q1=`hive -e "select percentile(cnt_pkg,0.25) from $tmp_anticheat_device_install_7days_pre where day = '$day';"`
q3=`hive -e "select percentile(cnt_pkg,0.75) from $tmp_anticheat_device_install_7days_pre where day = '$day';"`
maxValue=`hive -e "select percentile(cnt_pkg,0.99) from $tmp_anticheat_device_install_7days_pre where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '7',flag = '3')
select device,
case 
  when cnt_pkg <= ($q3+1.5*($q3-$q1)) then 0
  when cnt_pkg > ($q3+1.5*($q3-$q1)) and cnt_pkg <= $maxValue then cnt_pkg*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
  when cnt_pkg > $maxValue then 1
  end as riskScore
from $tmp_anticheat_device_install_7days_pre
where day = '$day';
"

#近14天评分
q1=`hive -e "select percentile(cnt_pkg,0.25) from $tmp_anticheat_device_install_14days_pre where day = '$day';"`
q3=`hive -e "select percentile(cnt_pkg,0.75) from $tmp_anticheat_device_install_14days_pre where day = '$day';"`
maxValue=`hive -e "select percentile(cnt_pkg,0.99) from $tmp_anticheat_device_install_14days_pre where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '14',flag = '3')
select device,
case 
  when cnt_pkg <= ($q3+1.5*($q3-$q1)) then 0
  when cnt_pkg > ($q3+1.5*($q3-$q1)) and cnt_pkg <= $maxValue then cnt_pkg*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
  when cnt_pkg > $maxValue then 1
  end as riskScore
from $tmp_anticheat_device_install_14days_pre
where day = '$day';
"

#近30天评分
q1=`hive -e "select percentile(cnt_pkg,0.25) from $tmp_anticheat_device_install_30days_pre where day = '$day';"`
q3=`hive -e "select percentile(cnt_pkg,0.75) from $tmp_anticheat_device_install_30days_pre where day = '$day';"`
maxValue=`hive -e "select percentile(cnt_pkg,0.99)from $tmp_anticheat_device_install_30days_pre where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition (day = '$day',timewindow = '30',flag = '3')
select device,
case 
  when cnt_pkg <= ($q3+1.5*($q3-$q1)) then 0
  when cnt_pkg > ($q3+1.5*($q3-$q1)) and cnt_pkg <= $maxValue then cnt_pkg*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
  when cnt_pkg > $maxValue then 1
  end as riskScore
from $tmp_anticheat_device_install_30days_pre
where day = '$day';
"