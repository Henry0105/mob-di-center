#!/bin/bash
: '
@owner:luost
@describe:近90天ip连接量评分
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
source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#dws_device_ip_info_di=dm_mobdi_topic.dws_device_ip_info_di

#tmp
tmp_anticheat_device_ip_cnt_90days=dw_mobdi_tmp.tmp_anticheat_device_ip_cnt_90days
tmp_anticheat_device_first3_ip_cnt_90days=dw_mobdi_tmp.tmp_anticheat_device_first3_ip_cnt_90days

#output
#label_l1_anticheat_device_riskscore=dm_mobdi_report.label_l1_anticheat_device_riskscore

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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

insert overwrite table $tmp_anticheat_device_ip_cnt_90days partition (day = '$day')
select device,count(1) as cnt
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
group by device;
"

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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

insert overwrite table $tmp_anticheat_device_first3_ip_cnt_90days partition (day = '$day')
select device, count(1) as cnt
from 
(
    select device
    from $dws_device_ip_info_di
    where day > '$p90day' 
    and day <= '$day' 
    and ipaddr <> '' 
    and ipaddr is not null
    group by device, concat_ws('.', split(ipaddr, '\\\\.')[0], split(ipaddr, '\\\\.')[1], split(ipaddr, '\\\\.')[2])
) a 
group by device;
"


#ip连接量评分
q1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_ip_cnt_90days where day = '$day';"`
q3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_ip_cnt_90days where day = '$day';"`
maxValue=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_ip_cnt_90days where day = '$day';"`

fq1=`hive -e "select percentile(cnt,0.25) from $tmp_anticheat_device_first3_ip_cnt_90days where day = '$day';"`
fq3=`hive -e "select percentile(cnt,0.75) from $tmp_anticheat_device_first3_ip_cnt_90days where day = '$day';"`
fmax=`hive -e "select percentile(cnt,0.99) from $tmp_anticheat_device_first3_ip_cnt_90days where day = '$day';"`

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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

insert overwrite table $label_l1_anticheat_device_riskscore partition(day = '$day',timewindow = '90',flag = '5')
select device,avg(riskScore) as riskScore
from 
(
    select device,
    case 
      when cnt <= ($q3+1.5*($q3-$q1)) then 0
      when cnt > ($q3+1.5*($q3-$q1)) and cnt <= $maxValue then cnt*1/($maxValue-($q3+1.5*($q3-$q1))) + (1-$maxValue*1/($maxValue-($q3+1.5*($q3-$q1))))
      when cnt > $maxValue then 1
      end as riskScore
    from $tmp_anticheat_device_ip_cnt_90days
    where day = '$day'

    union all

    select device,
    case 
      when cnt <= ($fq3+1.5*($fq3-$fq1)) then 0
      when cnt > ($fq3+1.5*($fq3-$fq1)) and cnt <= $fmax then cnt*1/($fmax-($fq3+1.5*($fq3-$fq1))) + (1-$fmax*1/($fmax-($fq3+1.5*($fq3-$fq1))))
      when cnt > $fmax then 1
      end as riskScore
    from $tmp_anticheat_device_first3_ip_cnt_90days
    where day = '$day'
) a
group by device;
"