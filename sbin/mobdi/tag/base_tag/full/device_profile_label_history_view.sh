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

source /home/dba/mobdi_center/conf/hive-env.sh

#rp_device_profile_full_history_view=rp_mobdi_report.rp_device_profile_full_history_view
#rp_device_profile_full_history_true=rp_mobdi_report.rp_device_profile_full_history_true
#device_profile_label_full_par=rp_mobdi_report.device_profile_label_full_par
#rp_device_profile_full_view=rp_mobdi_report.rp_device_profile_full_view

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
set hive.support.quoted.identifiers=None;

drop view if exists $rp_device_profile_full_history_view;
create view $rp_device_profile_full_history_view as
select \`(rn)?+.+\` from
(
select *,row_number() over (partition by device order by processtime_all desc,version desc ) as rn  from
(
    select * from $device_profile_label_full_par where version in ($arr1)
    union all
    select * from $rp_device_profile_full_view
)t
)history where rn=1
"

hive -v -e "
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.quoted.identifiers=None;

insert  overwrite table $rp_device_profile_full_history_true partition (version)
select \`(version)?+.+\`,'${day}.1000' as version from $rp_device_profile_full_history_view
"

for old_version in `hive -e "show partitions $rp_device_profile_full_history_true " | grep -v '_bak' | sort | head -n -5`
do
    echo "rm $old_version"
    hive -v -e "alter table $rp_device_profile_full_history_true drop if exists partition($old_version)"
done