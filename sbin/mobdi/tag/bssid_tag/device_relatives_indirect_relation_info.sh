#!/bin/bash
: '
@owner:luost
@describe:亲友二度共网关系
@projectName:mobdi
'

set -x -e

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive_db_tb_report.properties

#源表
#device_relatives_friends_relation_info_mi=rp_mobdi_app.device_relatives_friends_relation_info_mi

#输出表
#device_relatives_friends_indirect_relation_info=rp_mobdi_app.device_relatives_friends_indirect_relation_info

hive -v -e "
create table if not exists $device_relatives_friends_indirect_relation_info(
    device string comment '设备',
    device_indirect string comment '二度亲友关系设备',
    confidence_indirect double comment '二度亲友关系置信度'
)
comment '二度亲友关系表'
partitioned by (day string comment '日期')
stored as orc;
"

hive -v -e "
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx7200m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx7200m;
set hive.hadoop.supports.splittable.combineinputformat=true;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
set hive.exec.parallel=true;

--清洗一度关系设备数>100的数据
with device_etl as (
    select device,device_relative,confidence
    from
    (
        select device,device_relative,confidence,count(1) over(partition by device) as cnt
        from $device_relatives_friends_relation_info_mi
        where day = '$day'
    )a
    where cnt <=100
),

device_indirectDevice_confidence as
(
    select a.device,
           b.device_relative as device_indirect,
           a.confidence as confidence,
           b.confidence as indirect_confidence
    from device_etl a
    inner join device_etl b
    on a.device_relative = b.device
    where a.device <> b.device_relative
),

device_indirectDevice_etl as 
(
    select device,device_indirect,confidence,count(1) over(partition by device,device_indirect) as cnt
    from 
    (
        select a.device,a.device_indirect,a.confidence*a.indirect_confidence as confidence
        from device_indirectDevice_confidence a
        left join device_etl b
        on a.device = b.device and a.device_indirect = b.device_relative
        where b.device is null and b.device_relative is null
    )c
)

insert overwrite table $device_relatives_friends_indirect_relation_info partition (day = '$day')
select device,device_indirect,round(confidence,2) as confidence_indirect
from device_indirectDevice_etl
where cnt = 1

union all

select device,device_indirect,
       round(1-nvl(power(10, sum(log(10, 1-confidence))),0),2) as confidence_indirect
from device_indirectDevice_etl
where cnt > 1
group by device,device_indirect;
"