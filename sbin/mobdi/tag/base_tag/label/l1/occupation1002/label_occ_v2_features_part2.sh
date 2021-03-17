#!/bin/sh

set -x -e

: '
@owner:hugl
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

day=$1
tmpdb=${dw_mobdi_md}
appdb="rp_mobdi_report"
#input
device_applist_new=${dim_device_applist_new_di}
mapping_app_index="dm_sdk_mapping.mapping_app_income_index"

output_table=${tmpdb}.tmp_occ1002_predict_part2

## part2 mapping_app_index源数据不同,不能复用
HADOOP_USER_NAME=dba hive -e"
with seed as
(
    select *
	from $device_applist_new
	where day = '$day'
)
insert overwrite table ${output_table} partition(day='${day}')
select x.device
      ,if(y.device is null,array(0), y.index) index
        ,if(y.device is null,array(0.0), y.cnt) cnt
from
(
 select device from seed group by device
)x
left join
(
select device,collect_list(index) index,collect_list(cnt) cnt
from
(select a.device
, b.index ,1.0 cnt
from
seed a
join
(
select apppkg,index from $mapping_app_index where version='1000'
) b
on a.pkg=b.apppkg
)c group by device
)y
on x.device=y.device;
"
#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done