#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的网络标签 近一个月最喜欢使用的网络
@projectName:MOBDI
'
# 无model使用

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1
b30day=`date -d "$day -30 days" "+%Y%m%d"`

##input
#device_ip_info="dm_mobdi_master.device_ip_info"
device_ip_info=${dws_device_ip_info_di}

##output
label_l1_network_di=${label_l1_network_label_di}

# step 1 : 计算device的网络标签 近一个月最喜欢使用的网络
hive -v -e"
set hive.optimize.index.filter=true;
set hive.exec.orc.zerocopy=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.ppd=true;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=250000000;
set mapred.min.split.size.per.rack=250000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task=250000000;
set hive.exec.reducers.bytes.per.reducer=256000000;

with tmp_device_network_cnts as
(
  select device,network,count(1) cnt
  from
  (
    select device,network_single as network
    from $device_ip_info
    lateral view  explode(split(network,',')) t as network_single
    where (day between'$b30day' and '$day')
    and plat=1
    and network!=''
    and network is not null
  )a
  group by device,network
),
tmp_device_last_month_network as
(
  select device,network
  from
  (
    select device,network,
           row_number() over( partition by device order by cnt desc) as rn
    from tmp_device_network_cnts
  )tmp
  where rn=1
)
insert overwrite table $label_l1_network_di partition(day='$day')
select a.device,b.network
from
(
  select device
  from $device_ip_info
  where day='$day'
  and plat=1
  group by device
)a
left join
(
  select *
  from tmp_device_last_month_network
) b on a.device=b.device;
"
