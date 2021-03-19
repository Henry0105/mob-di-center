#!/bin/bash
: '
@owner:luost
@describe:亲友关系全量表
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
#device_relatives_friends_relation_info_mi=dm_mobdi_report.device_relatives_friends_relation_info_mi

#全量表
#device_relatives_friends_relation_full_mf=dm_mobdi_report.device_relatives_friends_relation_full_mf


relativePartition=`hive -e "show partitions $device_relatives_friends_relation_full_mf" | sort| tail -n 1`

hive -v -e "
create table if not exists $device_relatives_friends_relation_full_mf(
    device string comment '设备号',
    device_relative string comment '亲友关系设备',
    confidence double comment '置信度，0-1，越大关系越强',
    relative_type string comment '关系类型（0都不是家庭，可能是朋友；1有一方家庭，可能是亲戚；2双方家庭，可能是家人/邻居）',
    update_time string comment '更新时间'
)
comment '一度亲友关系设备网络全量表'
partitioned by (day string comment '日期')
stored as orc;
"

#亲友关系全量表数据生成
hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
 
--若a-b在上个全量表和增量表中都存在，权重分别为a和b：更新时间置为当前月，权重更新为 1-(1-b)(1-0.8a)
with bothIn_full_incr as 
(
    select a.device,
           a.device_relative,
           round((1-(1-b.confidence)*(1-0.8*a.confidence)),2) as confidence,
           a.relative_type,
           '$day' as update_time
    from 
    (
        select device,device_relative,confidence,relative_type
        from $device_relatives_friends_relation_full_mf
        where $relativePartition
    )a
    inner join
    (
        select device,device_relative,confidence,relative_type
        from $device_relatives_friends_relation_info_mi
        where day = '$day'
    )b 
    on a.device = b.device and a.device_relative = b.device_relative
),

--若a-b仅在增量表中存在，权重为b：更新时间置为当前月，a-b权重为增量表权重b。
onlyIn_incr as 
(
    select a.device,
           a.device_relative,
           a.confidence as confidence,
           a.relative_type,
           '$day' as update_time
    from 
    (
        select device,device_relative,confidence,relative_type
        from $device_relatives_friends_relation_info_mi
        where day = '$day'
    )a
    left join
    (
        select device,device_relative,confidence,relative_type
        from $device_relatives_friends_relation_full_mf
        where $relativePartition
    )b
    on a.device = b.device and a.device_relative = b.device_relative
    where b.device is null and b.device_relative is null
),

--若a-b仅在上个全量表中存在,权重为a，且更新时间小于一年：更新时间不变，权重衰减为 0.8*a
--若a-b仅在上个全量表中存在，且更新时间大于等于1年：删除a-b。
onlyIn_full as 
(
    select a.device,
           a.device_relative,
           round(a.confidence*0.8,2) as confidence,
           a.relative_type,
           a.update_time
    from 
    (
        select device,device_relative,confidence,relative_type,update_time
        from $device_relatives_friends_relation_full_mf
        where $relativePartition
    )a
    left join
    (
        select device,device_relative,confidence,relative_type
        from $device_relatives_friends_relation_info_mi
        where day = '$day'
    )b 
    on a.device = b.device and a.device_relative = b.device_relative
    where b.device is null and b.device_relative is null
    and datediff(
    to_date(from_unixtime(UNIX_TIMESTAMP('$day','yyyyMMdd'))),
    to_date(from_unixtime(UNIX_TIMESTAMP(a.update_time,'yyyyMMdd')))
    ) < 365
)

insert overwrite table $device_relatives_friends_relation_full_mf partition (day = '$day')
select device,device_relative,confidence,relative_type,update_time
from bothIn_full_incr

union all

select device,device_relative,confidence,relative_type,update_time
from onlyIn_incr

union all

select device,device_relative,confidence,relative_type,update_time
from onlyIn_full;
"