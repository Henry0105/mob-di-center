#!/bin/bash
set -x -e

: '
@owner:guanyt
@describe:device用户标签画像的历史视图
@projectName:MOBDI
@BusinessName:profile
'

if [ $# -lt 1 ]; then
     echo "ERROR: wrong number of parameters"
     echo "USAGE: '<date>'"
     exit 1
fi

day=$1

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh

tmpdb="dw_mobdi_tmp"
appdb="rp_mobdi_report"

init2(){
    t=$1
    the_day=`date -d "${t}" +%Y0101`
    early_day="20180101"

    arr1="'${t}.1000'"
    while [ "${the_day}" -ge "${early_day}" ]
    do
        arr1="${arr1},'${the_day}_monthly_bak'"
        the_day=`date -d "${the_day} last year" +"%Y0101"`
    done
}
init2 $day
echo ${arr1}

hive -v -e  "
set mapreduce.job.queuename=root.yarn_data_compliance2;
set hive.support.quoted.identifiers=None;

drop view if exists $appdb.rp_device_profile_full_history_view;
create view $appdb.rp_device_profile_full_history_view as
select \`(rn)?+.+\` from
(
select *,row_number() over (partition by device order by processtime_all desc,version desc ) as rn  from
(
    select * from $appdb.device_profile_label_full_par where version in ($arr1)
    union all
    select * from $appdb.rp_device_profile_full_view
)t
)history where rn=1
"

hive -v -e "
set mapreduce.job.queuename=root.yarn_data_compliance2;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=None;

insert  overwrite table $appdb.rp_device_profile_full_history_true partition (version)
select \`(version)?+.+\`,'${day}.1000' as version from $appdb.rp_device_profile_full_history_view
"

for old_version in `hive -e "show partitions $appdb.rp_device_profile_full_history_true " | grep -v '_bak' | sort | head -n -5`
do
    echo "rm $old_version"
    hive -v -e "alter table $appdb.rp_device_profile_full_history_true drop if exists partition($old_version)"
done