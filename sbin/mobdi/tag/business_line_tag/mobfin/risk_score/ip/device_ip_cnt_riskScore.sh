#!/bin/bash
: '
@owner:luost
@describe:ip连接量评分标签
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1
p7day=`date -d "$day -7 days" +%Y%m%d`
p14day=`date -d "$day -14 days" +%Y%m%d`
p30day=`date -d "$day -30 days" +%Y%m%d`

#导入配置文件
source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dw_mobdi_tmp

#源表
tmp_anticheat_device_ip_cnt_7days=$tmpdb.tmp_anticheat_device_ip_cnt_7days
tmp_anticheat_device_ip_cnt_14days=$tmpdb.tmp_anticheat_device_ip_cnt_14days
tmp_anticheat_device_ip_cnt_30days=$tmpdb.tmp_anticheat_device_ip_cnt_30days
tmp_anticheat_device_first3_ip_cnt_7days=$tmpdb.tmp_anticheat_device_first3_ip_cnt_7days
tmp_anticheat_device_first3_ip_cnt_14days=$tmpdb.tmp_anticheat_device_first3_ip_cnt_14days
tmp_anticheat_device_first3_ip_cnt_30days=$tmpdb.tmp_anticheat_device_first3_ip_cnt_30days

#输出表
#label_l1_anticheat_device_riskScore=dm_mobdi_report.label_l1_anticheat_device_riskScore

#近七天
q1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_ip_cnt_7days where day = '$day';"`
q3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_ip_cnt_7days where day = '$day';"`
maxValue=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_ip_cnt_7days where day = '$day';"`

fq1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_first3_ip_cnt_7days where day = '$day';"`
fq3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_first3_ip_cnt_7days where day = '$day';"`
fmax=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_first3_ip_cnt_7days where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition(day = '$day',timewindow = '7',flag = '5')
select device,avg(riskScore) as riskScore
from 
(
    select device,
    case 
      when cnt <= ($q3+1.5*($q3-$q1)) then 0
      when cnt > ($q3+1.5*($q3-$q1)) and cnt <= $maxValue then cnt*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
      when cnt > $maxValue then 1
      end as riskScore
    from $tmp_anticheat_device_ip_cnt_7days
    where day = '$day'

    union all

    select device,
    case 
      when cnt <= ($fq3+1.5*($fq3-$fq1)) then 0
      when cnt > ($fq3+1.5*($fq3-$fq1)) and cnt <= $fmax then cnt*1/($fmax-($fq3+1.5*($fq3-$fq1))) + (1-$fmax*1/($fmax-($fq3+1.5*($fq3-$fq1))))
      when cnt > $fmax then 1
      end as riskScore
    from $tmp_anticheat_device_first3_ip_cnt_7days
    where day = '$day'
) a
group by device;
"

#近14天
q1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_ip_cnt_14days where day = '$day';"`
q3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_ip_cnt_14days where day = '$day';"`
maxValue=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_ip_cnt_14days where day = '$day';"`

fq1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_first3_ip_cnt_14days where day = '$day';"`
fq3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_first3_ip_cnt_14days where day = '$day';"`
fmax=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_first3_ip_cnt_14days where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition(day = '$day',timewindow = '14',flag = '5')
select device,avg(riskScore) as riskScore
from 
(
    select device,
    case 
      when cnt <= ($q3+1.5*($q3-$q1)) then 0
      when cnt > ($q3+1.5*($q3-$q1)) and cnt <= $maxValue then cnt*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
      when cnt > $maxValue then 1
      end as riskScore
    from $tmp_anticheat_device_ip_cnt_14days
    where day = '$day'

    union all

    select device,
    case 
      when cnt <= ($fq3+1.5*($fq3-$fq1)) then 0
      when cnt > ($fq3+1.5*($fq3-$fq1)) and cnt <= $fmax then cnt*1/($fmax-($fq3+1.5*($fq3-$fq1))) + (1-$fmax*1/($fmax-($fq3+1.5*($fq3-$fq1))))
      when cnt > $fmax then 1
      end as riskScore
    from $tmp_anticheat_device_first3_ip_cnt_14days
    where day = '$day'
) a
group by device;
"

#近30天
q1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_ip_cnt_30days where day = '$day';"`
q3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_ip_cnt_30days where day = '$day';"`
maxValue=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_ip_cnt_30days where day = '$day';"`

fq1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_first3_ip_cnt_30days where day = '$day';"`
fq3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_first3_ip_cnt_30days where day = '$day';"`
fmax=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_first3_ip_cnt_30days where day = '$day';"`

hive -v -e "
set hive.exec.parallel=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;

insert overwrite table $label_l1_anticheat_device_riskScore partition(day = '$day',timewindow = '30',flag = '5')
select device,avg(riskScore) as riskScore
from 
(
    select device,
    case 
      when cnt <= ($q3+1.5*($q3-$q1)) then 0
      when cnt > ($q3+1.5*($q3-$q1)) and cnt <= $maxValue then cnt*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
      when cnt > $maxValue then 1
      end as riskScore
    from $tmp_anticheat_device_ip_cnt_30days
    where day = '$day'

    union all

    select device,
    case 
      when cnt <= ($fq3+1.5*($fq3-$fq1)) then 0
      when cnt > ($fq3+1.5*($fq3-$fq1)) and cnt <= $fmax then cnt*1/($fmax-($fq3+1.5*($fq3-$fq1))) + (1-$fmax*1/($fmax-($fq3+1.5*($fq3-$fq1))))
      when cnt > $fmax then 1
      end as riskScore
    from $tmp_anticheat_device_first3_ip_cnt_30days
    where day = '$day'
) a
group by device;
"