#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: device的bssid_cnt
@projectName:MOBDI
'

if [ $# -ne 1 ]; then
    echo "ERROR: wrong number of parameters"
    echo "USAGE: <day>"
    exit 1
fi

day=$1

source /home/dba/mobdi_center/conf/hive-env.sh
tmpdb=$dm_mobdi_tmp
appdb=$dm_mobdi_report

#input
device_applist_new=${dim_device_applist_new_di}

#mapping
#mapping_contacts_words_20000
#mapping_age_word_index

#tmp
tmp_score_part5_v3=${tmpdb}.tmp_score_part5_v3

## 结果临时表
output_table=${tmpdb}.tmp_score_part5

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance;
insert overwrite  table ${output_table} partition(day='${day}')
select device,
       if(size(collect_list(index))=0,collect_set(0),collect_list(index)) as index,
       if(size(collect_list(cnt))=0,collect_set(0.0),collect_list(cnt)) as cnt
from
(
  select a.device,
         if(b.index_before_chi is null,null,b.index_after_chi) index,
         if(b.index_before_chi is null,null,1.0) cnt
from
(
  select device,index_old
  from ${tmp_score_part5_v3}
  lateral view explode(index) n as index_old
  where day = '$day'
)a
left join (select * from $mapping_age_word_index where version='1000') b
on a.index_old = b.index_before_chi
)x group by device;
"

#只保留最近7个分区
for old_version in `hive -e "show partitions ${output_table} " | grep -v '_bak' | sort | head -n -7`
do
    echo "rm $old_version"
    hive -v -e "alter table ${output_table} drop if exists partition($old_version)"
done
