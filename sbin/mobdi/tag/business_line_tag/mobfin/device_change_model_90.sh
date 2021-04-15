#!/bin/bash
: '
@owner:luost
@describe:近90天设备是否改机
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
#dwd_device_info_df=dm_mobdi_master.dwd_device_info_df
#dwd_device_info_di=dm_mobdi_master.dwd_device_info_di

#输出表
#label_l1_anticheat_device_changemodel_mi=dm_mobdi_report.label_l1_anticheat_device_changemodel_mi

hive -v -e "
set mapreduce.map.memory.mb=4096;
set mapreduce.map.java.opts='-Xmx3680m';
set mapreduce.child.map.java.opts='-Xmx3680m';
set mapreduce.reduce.memory.mb=4096;
set mapreduce.reduce.java.opts='-Xmx3680m';
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

insert overwrite table $label_l1_anticheat_device_changemodel_mi partition (day = '$day')
select device,if(count(1)>1,1,0) as if_change
from 
(
    select device,factory,model
    from $dwd_device_info_df
    where version = '${p90day}.1000' 
    and plat='1'
    group by device,factory,model

    union all

    select device,factory,model_clean as model
    from $dwd_device_info_di
    where day > '$p90day'
    and day < '$day'
    and plat = '1'
    group by device,factory,model_clean
)a
group by device;
"

