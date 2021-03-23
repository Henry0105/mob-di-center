#!/bin/bash
: '
@owner:luost
@describe:近90天设备关联ip数
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
source /home/dba/mobdi_center/conf/hive_db_tb_master.properties
source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#输入表
#dws_device_ip_info_di=dm_mobdi_master.dws_device_ip_info_di

#输出表
#label_l1_anticheat_device_ip_mi=dm_mobdi_report.label_l1_anticheat_device_ip_mi

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
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

insert overwrite table $label_l1_anticheat_device_ip_mi partition (day = '$day')
select device,count(1) as cnt
from
(
    select device, ipaddr
    from $dws_device_ip_info_di
    where day > '$p90day' 
    and day <= '$day'
    group by device,ipaddr
) month_cnt
group by device;
"

