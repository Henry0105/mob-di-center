#!/bin/bash
set -x -e

: '
@owner:liuyanqiang
@describe:表最新分区生成后，立即进行qc，qc成功后，生成最新的view视图，才允许业务方将后续依赖的job启动
@projectName:QC
'

: '
  第一批qc表：rp_mobdi_app.device_profile_label_full_par、rp_mobdi_app.device_models_confidence_full、
              dm_mobdi_mapping.android_id_full、dm_mobdi_mapping.ios_id_full
  1.统计表的当前分区总数、当前分区主键总数、上一个分区总数
  2.如果当前分区主键数不等于当前分区总数或者当前分区总数小于等于上一个分区总数或者与上一个分区相比数据量波动超过10%，
    表示qc失败，程序已失败退出
  3.qc成功，返回
'

if [[ $# -lt 3 ]]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 3
fi

newVer=$1
lastVer=$2
table=$3

#统计当前分区总数、当前分区主键总数、上一个分区总数
result=`hive -e "
select total_counts,key_counts
from
(
  --统计当前分区总数和当前分区主键总数
  select sum(cnt) as total_counts,
         count(1) as key_counts
  from
  (
    select device,
           count(1) as cnt
    from ${table}
    where version='$newVer'
    group by device
  ) t
) now_data
"`

arr=(${result// / })
total_counts=${arr[0]}
key_counts=${arr[1]}

#当前分区主键数不等于当前分区总数
#或者当前分区总数小于等于上一个分区总数
#或者与上一个分区相比数据量波动超过10%，表示qc失败，程序已失败退出
if [[ ${total_counts} -ne ${key_counts} ]]; then
  #qc失败
  echo "ERROR: qc failed"
  exit 1
else
  #qc成功，返回
  echo "qc success"
fi