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
mapping_contacts_words_20000="dm_sdk_mapping.mapping_contacts_words_20000"

##取的v3版本
HADOOP_USER_NAME=dba hive -e"
set mapreduce.job.queuename=root.yarn_data_compliance2;
drop table if exists ${tmpdb}.tmp_occ1002_predict_part5;
create table ${tmpdb}.tmp_occ1002_predict_part5 stored as orc as
with seed as
(
  select device
  from $device_applist_new
  where day = '$day'
  group by device
)
select a.device,
      if(y.device is null,array(0), y.index) index
      ,if(y.device is null,array(0.0), y.cnt) cnt
from seed a  left join
(
select device,collect_list(index) index,collect_list(cnt) cnt
from
(select device,index,1.0 cnt
from
(select x.device,y.word_index
from
(
  select device,phone
  from
  (
    select *,row_number() over(partition by device order by pn_tm desc) rn
    from
    (
      select device,n.phone,n.pn_tm
      from
      (
        select a.device,concat(phone,'=',phone_ltm) phone_list
        from seed a
        join dim_mobdi_mapping.android_id_mapping_full_view b
        on a.device=b.device
      )c lateral view explode_tags(phone_list) n as phone,pn_tm
    )d       where length(phone) = 11
  )e where rn=1
)x
join
(select * from $mapping_contacts_words_20000 where version='20201222') y
on x.phone=y.phone
)xx
lateral view explode(word_index) n as index
)yy group by device
)y on a.device=y.device
;
"